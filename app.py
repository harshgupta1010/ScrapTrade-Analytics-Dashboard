import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gspread
from google.oauth2.service_account import Credentials
import os
import numpy as np
from datetime import date

st.set_page_config(page_title="ScrapTrade Analytics Dashboard", layout="wide")

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

SHEET_NAME = "ScrapTrade_Database"
GST_RATE = 0.18  # 18% GST (HSN 7204)

# ─────────────────────────────────────────────
# Google Sheets Helpers
# ─────────────────────────────────────────────

@st.cache_resource
def get_client():
    if os.path.exists("credentials.json"):
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPE)
    else:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=SCOPE
        )
    return gspread.authorize(creds)

def get_users_sheet():
    client = get_client()
    sheet = client.open(SHEET_NAME)
    return sheet.worksheet("users")

def get_lots_sheet():
    client = get_client()
    sheet = client.open(SHEET_NAME)
    return sheet.worksheet("lots")

def verify_login(username, password):
    ws = get_users_sheet()
    records = ws.get_all_records()
    for row in records:
        if row['username'] == username and str(row['password']) == str(password):
            return row['company_name']
    return None

# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────

def load_lots(username):
    ws = get_lots_sheet()
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame()
    df = df[df['username'] == username]
    if df.empty:
        return pd.DataFrame()
    df = df.drop(columns=['username'])

    # Backward compatible new columns
    if 'status'       not in df.columns: df['status']       = 'Sold'
    if 'buyer_name'   not in df.columns: df['buyer_name']   = ''
    if 'railway_zone' not in df.columns: df['railway_zone'] = ''

    df['date_of_purchase'] = pd.to_datetime(df['date_of_purchase'], format='mixed', dayfirst=False)
    df['date_of_sale']     = pd.to_datetime(df['date_of_sale'],     format='mixed', dayfirst=False, errors='coerce')

    for col in ['quantity_mt', 'purchase_price_per_mt', 'selling_price_per_mt',
                'labour_expenses', 'logistics_per_mt', 'hydra_charges',
                'commission_per_mt', 'other_expenses']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df

def add_lot(username, lot_data):
    ws = get_lots_sheet()
    row = [username] + list(lot_data.values())
    ws.append_row(row)

def delete_lot(username, lot_no):
    ws = get_lots_sheet()
    records = ws.get_all_records()
    for i, row in enumerate(records):
        if row['username'] == username and str(row['lot_no']) == str(lot_no):
            ws.delete_rows(i + 2)
            break

# ─────────────────────────────────────────────
# Metric Calculations
#
# EXACT FORMULA:
#   total_purchase_with_gst = base_purchase + input_gst   (GST on top of entered price)
#   total_sale_with_gst     = base_sale     + output_gst  (GST on top of entered price)
#   net_gst_payable         = output_gst - input_gst      (submitted to govt after ITC)
#
#   gross_profit = total_sale_with_gst - total_purchase_with_gst - net_gst_payable
#   net_profit   = gross_profit - all_expenses
# ─────────────────────────────────────────────

def calculate_metrics(df):
    df = df.copy()

    # Base values — entered prices exclude GST
    df['total_purchase_value'] = df['quantity_mt'] * df['purchase_price_per_mt']
    df['total_sale_value']     = df['quantity_mt'] * df['selling_price_per_mt']
    df['total_logistics']      = df['quantity_mt'] * df['logistics_per_mt']
    df['total_commission']     = df['quantity_mt'] * df['commission_per_mt']

    # GST added on top of base values
    df['input_gst']       = (df['total_purchase_value'] * GST_RATE).round(2)
    df['output_gst']      = (df['total_sale_value']     * GST_RATE).round(2)
    df['net_gst_payable'] = (df['output_gst'] - df['input_gst']).round(2)

    # Total cash flows incl. GST
    df['total_purchase_with_gst'] = df['total_purchase_value'] + df['input_gst']
    df['total_sale_with_gst']     = df['total_sale_value']     + df['output_gst']

    # Operational expenses — GST-free, entered as final amounts
    df['total_expenses'] = (
        df['labour_expenses']  +
        df['total_logistics']  +
        df['hydra_charges']    +
        df['total_commission'] +
        df['other_expenses']
    )

    # ── CORE PROFIT LOGIC ─────────────────────────────────────────────────────
    # Gross Profit = Sale incl.GST − Purchase incl.GST − Net GST Payable to Govt
    df['gross_profit'] = (
        df['total_sale_with_gst']
        - df['total_purchase_with_gst']
        - df['net_gst_payable']
    )
    # Net Profit = Gross Profit − All Expenses
    df['net_profit'] = df['gross_profit'] - df['total_expenses']
    # ─────────────────────────────────────────────────────────────────────────

    # Break-even base selling price/mt (excl. GST) where net_profit = 0
    # gross_profit = expenses  =>  (sale - purchase) * (1+GST) - net_gst = expenses
    # Simplifies to: selling_price = purchase_price + (expenses / qty)
    df['breakeven_price_per_mt'] = (
        df['purchase_price_per_mt'] + (df['total_expenses'] / df['quantity_mt'])
    ).round(2)

    df['profit_margin_pct'] = (
        (df['net_profit'] / df['total_sale_with_gst'] * 100)
        .where(df['total_sale_with_gst'] > 0, 0)
        .round(2)
    )
    df['profit_per_mt']      = (df['net_profit'] / df['quantity_mt']).round(2)
    df['deal_duration_days'] = (df['date_of_sale'] - df['date_of_purchase']).dt.days

    df['holding_days'] = df.apply(
        lambda r: (pd.Timestamp(date.today()) - r['date_of_purchase']).days
        if r['status'] == 'Pending' else r['deal_duration_days'], axis=1
    )
    return df

# ─────────────────────────────────────────────
# Monthly / Quarterly P&L Helper
# ─────────────────────────────────────────────

def build_pnl(df_sold):
    df_sold = df_sold.copy()
    df_sold['month']   = df_sold['date_of_sale'].dt.to_period('M').astype(str)
    df_sold['quarter'] = df_sold['date_of_sale'].dt.to_period('Q').astype(str)
    return df_sold

# ─────────────────────────────────────────────
# Linear Regression Forecast Helper
# ─────────────────────────────────────────────

def forecast_prices(df_sold, periods=3):
    df_s = df_sold.sort_values('date_of_purchase').reset_index(drop=True)
    x = np.arange(len(df_s))
    results = {}
    for col in ['purchase_price_per_mt', 'selling_price_per_mt']:
        y = df_s[col].values
        if len(y) >= 2:
            slope, intercept = np.polyfit(x, y, 1)
            future_x = np.arange(len(df_s), len(df_s) + periods)
            results[col] = slope * future_x + intercept
        else:
            results[col] = [None] * periods

    forecast_df = pd.DataFrame({
        'period':                [f"Forecast +{i+1}" for i in range(periods)],
        'purchase_price_per_mt': results['purchase_price_per_mt'],
        'selling_price_per_mt':  results['selling_price_per_mt'],
    })
    historical_df = df_s[['date_of_purchase', 'purchase_price_per_mt', 'selling_price_per_mt']].copy()
    historical_df['period'] = historical_df['date_of_purchase'].dt.strftime('%d %b %Y')
    return historical_df, forecast_df

# ─────────────────────────────────────────────
# Session State
# ─────────────────────────────────────────────

if 'logged_in'    not in st.session_state: st.session_state.logged_in    = False
if 'username'     not in st.session_state: st.session_state.username     = ""
if 'company_name' not in st.session_state: st.session_state.company_name = ""

# ─────────────────────────────────────────────
# LOGIN PAGE
# ─────────────────────────────────────────────

if not st.session_state.logged_in:
    st.title("ScrapTrade Analytics Dashboard")
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.subheader("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login", use_container_width=True):
            company = verify_login(username, password)
            if company:
                st.session_state.logged_in    = True
                st.session_state.username     = username
                st.session_state.company_name = company
                st.rerun()
            else:
                st.error("Invalid username or password!")

# ─────────────────────────────────────────────
# MAIN DASHBOARD
# ─────────────────────────────────────────────

else:
    st.title("ScrapTrade Analytics Dashboard")
    st.markdown(f"**Company: {st.session_state.company_name}**")

    if st.button("Logout", type="primary"):
        st.session_state.logged_in    = False
        st.session_state.username     = ""
        st.session_state.company_name = ""
        st.rerun()

    st.divider()

    # ── SIDEBAR ──────────────────────────────
    with st.sidebar:
        st.header("Add New Lot")
        lot_no           = st.text_input("Lot Number", placeholder="e.g. 101 or L-23")
        area             = st.text_input("Area / Location", placeholder="e.g. Indore")
        railway_zone     = st.selectbox("Railway Zone", ["", "WR", "CR", "NR", "ER", "SR", "SCR", "SER", "NWR", "ECR", "WCR", "NCR", "NER", "NFR", "Other"])
        buyer_name       = st.text_input("Buyer Name", placeholder="e.g. Mehta Steels")
        status           = st.selectbox("Lot Status", ["Sold", "Pending"])
        date_of_purchase = st.date_input("Date of Purchase")
        date_of_sale     = st.date_input("Date of Sale") if status == "Sold" else None

        quantity_mt    = st.number_input("Quantity (mt)", min_value=0.0, step=0.5)
        st.subheader("Pricing (Rs/mt) — Enter EXCLUDING GST")
        purchase_price = st.number_input("Purchase Price (Rs/mt)", min_value=0)
        selling_price  = st.number_input("Selling Price (Rs/mt)",  min_value=0) if status == "Sold" else 0
        st.subheader("Expenses — GST-free final amounts")
        labour     = st.number_input("Labour Expenses (Rs)",  min_value=0)
        logistics  = st.number_input("Logistics (Rs/mt)",     min_value=0)
        hydra      = st.number_input("Hydra Charges (Rs)",    min_value=0)
        commission = st.number_input("Commission (Rs/mt)",    min_value=0)
        other      = st.number_input("Other Expenses (Rs)",   min_value=0)

        # Live Preview
        if quantity_mt > 0 and selling_price > 0 and purchase_price > 0 and status == "Sold":
            prev_base_purchase = quantity_mt * purchase_price
            prev_base_sale     = quantity_mt * selling_price
            prev_input_gst     = prev_base_purchase * GST_RATE
            prev_output_gst    = prev_base_sale     * GST_RATE
            prev_net_gst       = prev_output_gst - prev_input_gst
            prev_purchase_gst  = prev_base_purchase + prev_input_gst
            prev_sale_gst      = prev_base_sale     + prev_output_gst
            prev_logistics     = quantity_mt * logistics
            prev_commission    = quantity_mt * commission
            prev_expenses      = labour + prev_logistics + hydra + prev_commission + other
            prev_gross         = prev_sale_gst - prev_purchase_gst - prev_net_gst
            prev_net           = prev_gross - prev_expenses
            prev_breakeven     = purchase_price + (prev_expenses / quantity_mt)

            st.divider()
            st.subheader("📊 Live Preview")
            st.markdown("**Purchase**")
            st.info(f"Base Purchase Value: Rs {prev_base_purchase:,.0f}")
            st.info(f"Input GST (18%): Rs {prev_input_gst:,.0f}")
            st.info(f"Total Purchase incl. GST: Rs {prev_purchase_gst:,.0f}")
            st.markdown("**Sale**")
            st.info(f"Base Sale Value: Rs {prev_base_sale:,.0f}")
            st.info(f"Output GST (18%): Rs {prev_output_gst:,.0f}")
            st.info(f"Total Sale incl. GST: Rs {prev_sale_gst:,.0f}")
            st.markdown("**GST**")
            st.warning(f"Net GST to Govt (Output − Input): Rs {prev_net_gst:,.0f}")
            st.markdown("**Profit**")
            st.info(f"Break-even Selling Price: Rs {prev_breakeven:,.0f}/mt")
            st.info(f"Total Expenses: Rs {prev_expenses:,.0f}")
            if prev_gross >= 0:
                st.success(f"Gross Profit: Rs {prev_gross:,.0f}")
            else:
                st.error(f"Gross Loss: Rs {prev_gross:,.0f}")
            if prev_net >= 0:
                st.success(f"Est. Net Profit: Rs {prev_net:,.0f}")
            else:
                st.error(f"Est. Net Loss: Rs {prev_net:,.0f}")

        st.divider()
        if st.button("Add Lot", use_container_width=True):
            if lot_no and area and quantity_mt > 0:
                lot_data = {
                    'lot_no':                lot_no,
                    'area':                  area,
                    'railway_zone':          railway_zone,
                    'buyer_name':            buyer_name,
                    'status':                status,
                    'date_of_purchase':      str(date_of_purchase),
                    'date_of_sale':          str(date_of_sale) if date_of_sale else '',
                    'quantity_mt':           quantity_mt,
                    'purchase_price_per_mt': purchase_price,
                    'selling_price_per_mt':  selling_price,
                    'labour_expenses':       labour,
                    'logistics_per_mt':      logistics,
                    'hydra_charges':         hydra,
                    'commission_per_mt':     commission,
                    'other_expenses':        other
                }
                add_lot(st.session_state.username, lot_data)
                st.success(f"Lot {lot_no} added successfully!")
                st.rerun()
            else:
                st.error("Please fill Lot No, Area and Quantity!")

        st.divider()
        st.header("Delete a Lot")
        df_check = load_lots(st.session_state.username)
        if not df_check.empty:
            lot_list      = df_check['lot_no'].tolist()
            lot_to_delete = st.selectbox("Select Lot to Delete", lot_list)
            if st.button("Delete Lot", use_container_width=True):
                delete_lot(st.session_state.username, lot_to_delete)
                st.success(f"Lot {lot_to_delete} deleted!")
                st.rerun()
        else:
            st.info("No lots to delete yet.")

    # ── LOAD & COMPUTE ───────────────────────
    df = load_lots(st.session_state.username)

    if df.empty:
        st.info("No data yet. Add your first lot from the sidebar!")
    else:
        df      = calculate_metrics(df)
        df_sold = df[df['status'] == 'Sold'].copy()
        df_pend = df[df['status'] == 'Pending'].copy()

        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Overview",
            "📦 Inventory",
            "🏭 Buyer & Zone Analysis",
            "📅 Monthly / Quarterly P&L",
            "📈 Price Forecast"
        ])

        # ════════════════════════════════════════
        # TAB 1 – OVERVIEW
        # ════════════════════════════════════════
        with tab1:
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Total Lots",       len(df))
            c2.metric("Sold Lots",        len(df_sold))
            c3.metric("Pending Lots",     len(df_pend))
            c4.metric("Total Revenue",    f"Rs {df_sold['total_sale_with_gst'].sum():,.0f}" if len(df_sold) > 0 else "—")
            c5.metric("Total Net Profit", f"Rs {df_sold['net_profit'].sum():,.0f}"           if len(df_sold) > 0 else "—")
            c6.metric("Avg Net Margin",   f"{df_sold['profit_margin_pct'].mean():.2f}%"      if len(df_sold) > 0 else "—")

            st.divider()

            if len(df_sold) > 0:
                g1, g2, g3 = st.columns(3)
                g1.metric("Total Input GST Paid",       f"Rs {df_sold['input_gst'].sum():,.0f}")
                g2.metric("Total Output GST Collected", f"Rs {df_sold['output_gst'].sum():,.0f}")
                g3.metric("Net GST Payable to Govt",    f"Rs {df_sold['net_gst_payable'].sum():,.0f}")

            st.divider()

            if len(df_sold) > 0:
                col1, col2 = st.columns(2)
                with col1:
                    fig1 = px.bar(df_sold, x='lot_no', y='net_profit', color='area',
                                  title='Net Profit per Lot',
                                  labels={'lot_no': 'Lot No', 'net_profit': 'Net Profit (Rs)'},
                                  text='net_profit',
                                  color_discrete_sequence=px.colors.qualitative.Set2)
                    fig1.update_traces(texttemplate='Rs %{text:,.0f}', textposition='outside')
                    fig1.update_layout(height=400)
                    st.plotly_chart(fig1, use_container_width=True)

                with col2:
                    fig2 = go.Figure()
                    fig2.add_trace(go.Scatter(x=df_sold['date_of_purchase'], y=df_sold['purchase_price_per_mt'],
                                              mode='lines+markers', name='Purchase Price (Rs/mt)',
                                              line=dict(color='red', width=2)))
                    fig2.add_trace(go.Scatter(x=df_sold['date_of_purchase'], y=df_sold['selling_price_per_mt'],
                                              mode='lines+markers', name='Selling Price (Rs/mt)',
                                              line=dict(color='green', width=2)))
                    fig2.update_layout(title='Purchase vs Selling Price Trend',
                                       xaxis_title='Date', yaxis_title='Price (Rs/mt)', height=400)
                    st.plotly_chart(fig2, use_container_width=True)

                col3, col4 = st.columns(2)
                with col3:
                    expense_labels = ['Labour', 'Logistics', 'Hydra', 'Commission', 'Other']
                    expense_totals = [
                        df_sold['labour_expenses'].sum(), df_sold['total_logistics'].sum(),
                        df_sold['hydra_charges'].sum(),   df_sold['total_commission'].sum(),
                        df_sold['other_expenses'].sum()
                    ]
                    fig3 = px.pie(names=expense_labels, values=expense_totals,
                                  title='Total Expense Breakdown',
                                  color_discrete_sequence=px.colors.qualitative.Pastel, hole=0.3)
                    fig3.update_traces(textinfo='percent+label')
                    fig3.update_layout(height=400)
                    st.plotly_chart(fig3, use_container_width=True)

                with col4:
                    fig4 = px.line(df_sold, x='date_of_purchase', y='profit_margin_pct', markers=True,
                                   title='Net Profit Margin % over Time',
                                   labels={'date_of_purchase': 'Date', 'profit_margin_pct': 'Margin (%)'},
                                   color_discrete_sequence=['#6366f1'])
                    fig4.add_hline(y=df_sold['profit_margin_pct'].mean(), line_dash='dash',
                                   line_color='orange',
                                   annotation_text=f"Avg: {df_sold['profit_margin_pct'].mean():.2f}%")
                    fig4.update_layout(height=400)
                    st.plotly_chart(fig4, use_container_width=True)

                col5, col6 = st.columns(2)
                with col5:
                    fig5 = px.scatter(df_sold, x='quantity_mt', y='net_profit',
                                      size='profit_margin_pct', color='area',
                                      hover_data=['lot_no', 'profit_per_mt'],
                                      title='Quantity (mt) vs Net Profit',
                                      color_discrete_sequence=px.colors.qualitative.Bold)
                    fig5.update_layout(height=400)
                    st.plotly_chart(fig5, use_container_width=True)

                with col6:
                    fig6 = go.Figure()
                    for label, col_name, color in [
                        ('Labour',     'labour_expenses',  '#f87171'),
                        ('Logistics',  'total_logistics',  '#fb923c'),
                        ('Hydra',      'hydra_charges',    '#fbbf24'),
                        ('Commission', 'total_commission', '#34d399'),
                        ('Other',      'other_expenses',   '#60a5fa'),
                    ]:
                        fig6.add_trace(go.Bar(name=label, x=df_sold['lot_no'],
                                              y=df_sold[col_name], marker_color=color))
                    fig6.update_layout(barmode='stack', title='Expense Breakdown per Lot',
                                       xaxis_title='Lot No', yaxis_title='Expenses (Rs)', height=400)
                    st.plotly_chart(fig6, use_container_width=True)

            st.divider()
            st.subheader("Complete Deal Summary")
            summary_cols = [
                'lot_no', 'area', 'railway_zone', 'buyer_name', 'status', 'quantity_mt',
                'purchase_price_per_mt', 'selling_price_per_mt',
                'total_purchase_value', 'input_gst',  'total_purchase_with_gst',
                'total_sale_value',     'output_gst', 'total_sale_with_gst',
                'net_gst_payable',
                'labour_expenses', 'total_logistics', 'hydra_charges',
                'total_commission', 'other_expenses', 'total_expenses',
                'gross_profit', 'net_profit',
                'breakeven_price_per_mt', 'profit_margin_pct', 'profit_per_mt', 'deal_duration_days'
            ]
            summary = df[summary_cols].copy()
            summary.columns = [
                'Lot No', 'Area', 'Zone', 'Buyer', 'Status', 'Qty (mt)',
                'Purchase Rs/mt', 'Selling Rs/mt',
                'Base Purchase (Rs)', 'Input GST (Rs)', 'Purchase incl.GST (Rs)',
                'Base Sale (Rs)',     'Output GST (Rs)', 'Sale incl.GST (Rs)',
                'Net GST to Govt (Rs)',
                'Labour (Rs)', 'Logistics (Rs)', 'Hydra (Rs)',
                'Commission (Rs)', 'Other (Rs)', 'Total Expenses (Rs)',
                'Gross Profit (Rs)', 'Net Profit (Rs)',
                'Breakeven Rs/mt', 'Net Margin %', 'Profit/mt (Rs)', 'Deal Days'
            ]
            st.dataframe(summary, use_container_width=True)

            st.divider()
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(label="Download Data as CSV",
                               data=csv, file_name='scrap_data_export.csv', mime='text/csv')

        # ════════════════════════════════════════
        # TAB 2 – INVENTORY
        # ════════════════════════════════════════
        with tab2:
            st.subheader("📦 Inventory — Pending / Unsold Lots")
            if df_pend.empty:
                st.success("No pending lots! All inventory has been sold.")
            else:
                k1, k2, k3 = st.columns(3)
                k1.metric("Pending Lots",              len(df_pend))
                k2.metric("Total Inventory (mt)",      f"{df_pend['quantity_mt'].sum():.1f} mt")
                k3.metric("Capital Locked incl. GST",  f"Rs {df_pend['total_purchase_with_gst'].sum():,.0f}")

                st.divider()

                fig_inv = px.bar(df_pend, x='lot_no', y='holding_days', color='railway_zone',
                                 title='Holding Days per Pending Lot',
                                 labels={'lot_no': 'Lot No', 'holding_days': 'Days Held'},
                                 text='holding_days',
                                 color_discrete_sequence=px.colors.qualitative.Safe)
                fig_inv.update_traces(textposition='outside')
                fig_inv.update_layout(height=380)
                st.plotly_chart(fig_inv, use_container_width=True)

                st.subheader("Break-Even Selling Price for Pending Lots")
                st.caption("Minimum base selling price/mt (excl. GST) to cover purchase cost + all expenses with zero net profit")
                pend_table = df_pend[[
                    'lot_no', 'area', 'railway_zone', 'quantity_mt',
                    'purchase_price_per_mt', 'breakeven_price_per_mt',
                    'total_purchase_with_gst', 'total_expenses', 'holding_days'
                ]].copy()
                pend_table.columns = [
                    'Lot No', 'Area', 'Zone', 'Qty (mt)',
                    'Purchase Rs/mt', 'Breakeven Rs/mt',
                    'Capital Locked incl.GST (Rs)', 'Total Expenses (Rs)', 'Days Held'
                ]
                st.dataframe(pend_table, use_container_width=True)

        # ════════════════════════════════════════
        # TAB 3 – BUYER & ZONE ANALYSIS
        # ════════════════════════════════════════
        with tab3:
            st.subheader("🏭 Buyer & Railway Zone Analysis")
            if df_sold.empty:
                st.info("No sold lots yet to analyse.")
            else:
                col_b, col_z = st.columns(2)

                with col_b:
                    st.markdown("#### By Buyer")
                    buyer_df = df_sold[df_sold['buyer_name'] != ''].groupby('buyer_name').agg(
                        Lots       = ('lot_no',             'count'),
                        Total_Qty  = ('quantity_mt',         'sum'),
                        Total_Rev  = ('total_sale_with_gst', 'sum'),
                        Net_Profit = ('net_profit',          'sum'),
                        Avg_Margin = ('profit_margin_pct',   'mean')
                    ).reset_index().sort_values('Net_Profit', ascending=False)

                    if buyer_df.empty:
                        st.info("No buyer names entered yet.")
                    else:
                        fig_b = px.bar(buyer_df, x='buyer_name', y='Net_Profit',
                                       color='Avg_Margin', title='Net Profit by Buyer',
                                       labels={'buyer_name': 'Buyer', 'Net_Profit': 'Net Profit (Rs)'},
                                       text='Net_Profit', color_continuous_scale='RdYlGn')
                        fig_b.update_traces(texttemplate='Rs %{text:,.0f}', textposition='outside')
                        fig_b.update_layout(height=380)
                        st.plotly_chart(fig_b, use_container_width=True)
                        buyer_df.columns = ['Buyer', 'Lots', 'Qty (mt)', 'Revenue incl.GST (Rs)', 'Net Profit (Rs)', 'Avg Margin %']
                        st.dataframe(buyer_df, use_container_width=True)

                with col_z:
                    st.markdown("#### By Railway Zone")
                    zone_df = df_sold[df_sold['railway_zone'] != ''].groupby('railway_zone').agg(
                        Lots         = ('lot_no',               'count'),
                        Total_Qty    = ('quantity_mt',           'sum'),
                        Avg_Purchase = ('purchase_price_per_mt', 'mean'),
                        Avg_Selling  = ('selling_price_per_mt',  'mean'),
                        Net_Profit   = ('net_profit',            'sum'),
                        Avg_Margin   = ('profit_margin_pct',     'mean')
                    ).reset_index().sort_values('Net_Profit', ascending=False)

                    if zone_df.empty:
                        st.info("No railway zones entered yet.")
                    else:
                        fig_z = px.bar(zone_df, x='railway_zone', y='Net_Profit',
                                       color='Avg_Margin', title='Net Profit by Railway Zone',
                                       labels={'railway_zone': 'Zone', 'Net_Profit': 'Net Profit (Rs)'},
                                       text='Net_Profit', color_continuous_scale='Blues')
                        fig_z.update_traces(texttemplate='Rs %{text:,.0f}', textposition='outside')
                        fig_z.update_layout(height=380)
                        st.plotly_chart(fig_z, use_container_width=True)

                        fig_zp = go.Figure()
                        fig_zp.add_trace(go.Bar(name='Avg Purchase Rs/mt', x=zone_df['railway_zone'],
                                                y=zone_df['Avg_Purchase'].round(0), marker_color='#f87171'))
                        fig_zp.add_trace(go.Bar(name='Avg Selling Rs/mt',  x=zone_df['railway_zone'],
                                                y=zone_df['Avg_Selling'].round(0),  marker_color='#34d399'))
                        fig_zp.update_layout(barmode='group', title='Avg Purchase vs Selling Price by Zone',
                                             xaxis_title='Zone', yaxis_title='Price (Rs/mt)', height=350)
                        st.plotly_chart(fig_zp, use_container_width=True)

        # ════════════════════════════════════════
        # TAB 4 – MONTHLY / QUARTERLY P&L
        # ════════════════════════════════════════
        with tab4:
            st.subheader("📅 Monthly & Quarterly P&L Summary")
            if df_sold.empty:
                st.info("No sold lots yet.")
            else:
                df_pnl     = build_pnl(df_sold)
                pnl_period = st.radio("View by", ["Monthly", "Quarterly"], horizontal=True)
                period_col = 'month' if pnl_period == "Monthly" else 'quarter'

                pnl_grouped = df_pnl.groupby(period_col).agg(
                    Lots            = ('lot_no',                'count'),
                    Total_Qty       = ('quantity_mt',            'sum'),
                    Revenue         = ('total_sale_with_gst',    'sum'),
                    Purchase_Cost   = ('total_purchase_with_gst','sum'),
                    Input_GST       = ('input_gst',              'sum'),
                    Output_GST      = ('output_gst',             'sum'),
                    Net_GST_Payable = ('net_gst_payable',        'sum'),
                    Total_Expenses  = ('total_expenses',         'sum'),
                    Gross_Profit    = ('gross_profit',           'sum'),
                    Net_Profit      = ('net_profit',             'sum'),
                    Avg_Margin      = ('profit_margin_pct',      'mean')
                ).reset_index().rename(columns={period_col: 'Period'})

                fig_pnl = go.Figure()
                fig_pnl.add_trace(go.Bar(name='Revenue incl.GST', x=pnl_grouped['Period'],
                                         y=pnl_grouped['Revenue'],      marker_color='#60a5fa'))
                fig_pnl.add_trace(go.Bar(name='Gross Profit',     x=pnl_grouped['Period'],
                                         y=pnl_grouped['Gross_Profit'], marker_color='#a78bfa'))
                fig_pnl.add_trace(go.Bar(name='Net Profit',       x=pnl_grouped['Period'],
                                         y=pnl_grouped['Net_Profit'],   marker_color='#34d399'))
                fig_pnl.add_trace(go.Bar(name='Net GST to Govt',  x=pnl_grouped['Period'],
                                         y=pnl_grouped['Net_GST_Payable'], marker_color='#fbbf24'))
                fig_pnl.update_layout(barmode='group',
                                      title=f"{pnl_period} Revenue, Gross Profit, Net Profit & GST",
                                      xaxis_title='Period', yaxis_title='Rs', height=420)
                st.plotly_chart(fig_pnl, use_container_width=True)

                fig_mg = px.line(pnl_grouped, x='Period', y='Avg_Margin', markers=True,
                                 title=f"{pnl_period} Average Net Profit Margin %",
                                 labels={'Avg_Margin': 'Avg Margin (%)'},
                                 color_discrete_sequence=['#a78bfa'])
                fig_mg.add_hline(y=pnl_grouped['Avg_Margin'].mean(), line_dash='dash', line_color='orange',
                                 annotation_text=f"Overall Avg: {pnl_grouped['Avg_Margin'].mean():.2f}%")
                fig_mg.update_layout(height=340)
                st.plotly_chart(fig_mg, use_container_width=True)

                st.subheader(f"{pnl_period} P&L Table")
                pnl_display = pnl_grouped.copy()
                for c in ['Revenue', 'Purchase_Cost', 'Input_GST', 'Output_GST',
                          'Net_GST_Payable', 'Total_Expenses', 'Gross_Profit', 'Net_Profit']:
                    pnl_display[c] = pnl_display[c].map(lambda x: f"Rs {x:,.0f}")
                pnl_display['Avg_Margin'] = pnl_display['Avg_Margin'].map(lambda x: f"{x:.2f}%")
                pnl_display.columns = [
                    'Period', 'Lots', 'Qty (mt)',
                    'Revenue incl.GST', 'Purchase incl.GST',
                    'Input GST', 'Output GST', 'Net GST to Govt',
                    'Total Expenses', 'Gross Profit', 'Net Profit', 'Avg Net Margin'
                ]
                st.dataframe(pnl_display, use_container_width=True)

        # ════════════════════════════════════════
        # TAB 5 – PRICE FORECAST
        # ════════════════════════════════════════
        with tab5:
            st.subheader("📈 Price Forecast (Linear Regression)")
            st.caption("Forecasts purchase and selling price trends based on historical data using linear regression.")

            if len(df_sold) < 2:
                st.warning("Need at least 2 sold lots to generate a forecast. Add more data!")
            else:
                forecast_periods = st.slider("Forecast how many future lots ahead?", min_value=1, max_value=10, value=3)
                hist_df, fore_df = forecast_prices(df_sold, periods=forecast_periods)

                fig_fc = go.Figure()
                fig_fc.add_trace(go.Scatter(x=hist_df['period'], y=hist_df['purchase_price_per_mt'],
                                            mode='lines+markers', name='Purchase Price (Actual)',
                                            line=dict(color='#f87171', width=2)))
                fig_fc.add_trace(go.Scatter(x=hist_df['period'], y=hist_df['selling_price_per_mt'],
                                            mode='lines+markers', name='Selling Price (Actual)',
                                            line=dict(color='#34d399', width=2)))
                fig_fc.add_trace(go.Scatter(x=fore_df['period'], y=fore_df['purchase_price_per_mt'],
                                            mode='lines+markers', name='Purchase Price (Forecast)',
                                            line=dict(color='#f87171', width=2, dash='dash'),
                                            marker=dict(symbol='diamond', size=9)))
                fig_fc.add_trace(go.Scatter(x=fore_df['period'], y=fore_df['selling_price_per_mt'],
                                            mode='lines+markers', name='Selling Price (Forecast)',
                                            line=dict(color='#34d399', width=2, dash='dash'),
                                            marker=dict(symbol='diamond', size=9)))
                fig_fc.add_vline(x=len(hist_df) - 0.5, line_dash='dot', line_color='gray',
                                 annotation_text='← Actual | Forecast →', annotation_position='top')
                fig_fc.update_layout(title='Purchase & Selling Price: Historical + Forecast',
                                     xaxis_title='Lot / Period', yaxis_title='Price (Rs/mt)', height=450,
                                     legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
                st.plotly_chart(fig_fc, use_container_width=True)

                st.subheader("Forecasted Prices")
                fore_display = fore_df[['period', 'purchase_price_per_mt', 'selling_price_per_mt']].copy()
                fore_display['est_margin_pct'] = (
                    (fore_display['selling_price_per_mt'] - fore_display['purchase_price_per_mt'])
                    / fore_display['selling_price_per_mt'] * 100
                ).round(2)
                fore_display.columns = ['Period', 'Forecast Purchase Rs/mt', 'Forecast Selling Rs/mt', 'Est. Gross Margin %']
                fore_display['Forecast Purchase Rs/mt'] = fore_display['Forecast Purchase Rs/mt'].map(lambda x: f"Rs {x:,.0f}" if x is not None else "N/A")
                fore_display['Forecast Selling Rs/mt']  = fore_display['Forecast Selling Rs/mt'].map( lambda x: f"Rs {x:,.0f}" if x is not None else "N/A")
                fore_display['Est. Gross Margin %']     = fore_display['Est. Gross Margin %'].map(    lambda x: f"{x:.2f}%"   if x is not None else "N/A")
                st.dataframe(fore_display, use_container_width=True)

                st.info("💡 **How it works:** Linear regression fits a trend line through your historical prices. If the selling price trend is rising, buying and holding inventory longer may improve margins. Use this to plan procurement timing.")
