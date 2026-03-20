import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gspread
from google.oauth2.service_account import Credentials
import json
import os

st.set_page_config(page_title="ScrapTrade Analytics Dashboard", layout="wide")

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

SHEET_NAME = "ScrapTrade_Database"

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
    df['date_of_purchase'] = pd.to_datetime(df['date_of_purchase'], format='mixed', dayfirst=False)
    df['date_of_sale'] = pd.to_datetime(df['date_of_sale'], format='mixed', dayfirst=False)
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

def calculate_metrics(df):
    df['total_purchase_value'] = df['quantity_mt'] * df['purchase_price_per_mt']
    df['total_sale_value'] = df['quantity_mt'] * df['selling_price_per_mt']
    df['total_logistics'] = df['quantity_mt'] * df['logistics_per_mt']
    df['total_commission'] = df['quantity_mt'] * df['commission_per_mt']
    df['total_expenses'] = (
        df['labour_expenses'] +
        df['total_logistics'] +
        df['hydra_charges'] +
        df['total_commission'] +
        df['other_expenses']
    )
    df['gross_profit'] = df['total_sale_value'] - df['total_purchase_value']
    df['net_profit'] = df['gross_profit'] - df['total_expenses']
    df['profit_margin_pct'] = (df['net_profit'] / df['total_sale_value'] * 100).round(2)
    df['profit_per_mt'] = (df['net_profit'] / df['quantity_mt']).round(2)
    df['deal_duration_days'] = (df['date_of_sale'] - df['date_of_purchase']).dt.days
    return df

# Login State
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = ""
if 'company_name' not in st.session_state:
    st.session_state.company_name = ""

# Login Page
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
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.company_name = company
                st.rerun()
            else:
                st.error("Invalid username or password!")

# Main Dashboard
else:
    st.title("ScrapTrade Analytics Dashboard")
    st.markdown(f"**Company: {st.session_state.company_name}**")

    if st.button("Logout", type="primary"):
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.session_state.company_name = ""
        st.rerun()

    st.divider()

    with st.sidebar:
        st.header("Add New Lot")
        lot_no = st.text_input("Lot Number", placeholder="e.g. 101 or L-23")
        area = st.text_input("Area / Location", placeholder="e.g. Indore")
        date_of_purchase = st.date_input("Date of Purchase")
        date_of_sale = st.date_input("Date of Sale")
        quantity_mt = st.number_input("Quantity (mt)", min_value=0.0, step=0.5)
        st.subheader("Pricing (Rs/mt)")
        purchase_price = st.number_input("Purchase Price (Rs/mt)", min_value=0)
        selling_price = st.number_input("Selling Price (Rs/mt)", min_value=0)
        st.subheader("Expenses")
        labour = st.number_input("Labour Expenses (Rs)", min_value=0)
        logistics = st.number_input("Logistics (Rs/mt)", min_value=0)
        hydra = st.number_input("Hydra Charges (Rs)", min_value=0)
        commission = st.number_input("Commission (Rs/mt)", min_value=0)
        other = st.number_input("Other Expenses (Rs)", min_value=0)

        if quantity_mt > 0 and selling_price > 0 and purchase_price > 0:
            preview_purchase = quantity_mt * purchase_price
            preview_sale = quantity_mt * selling_price
            preview_logistics = quantity_mt * logistics
            preview_commission = quantity_mt * commission
            preview_net = (preview_sale - preview_purchase) - labour - preview_logistics - hydra - preview_commission - other
            st.divider()
            st.subheader("Live Preview")
            st.info(f"Total Purchase Value: Rs {preview_purchase:,.0f}")
            st.info(f"Total Sale Value: Rs {preview_sale:,.0f}")
            st.info(f"Total Logistics: Rs {preview_logistics:,.0f}")
            st.info(f"Total Commission: Rs {preview_commission:,.0f}")
            if preview_net >= 0:
                st.success(f"Est. Net Profit: Rs {preview_net:,.0f}")
            else:
                st.error(f"Est. Net Loss: Rs {preview_net:,.0f}")

        st.divider()
        if st.button("Add Lot", use_container_width=True):
            if lot_no and area and quantity_mt > 0:
                lot_data = {
                    'lot_no': lot_no,
                    'area': area,
                    'date_of_purchase': str(date_of_purchase),
                    'date_of_sale': str(date_of_sale),
                    'quantity_mt': quantity_mt,
                    'purchase_price_per_mt': purchase_price,
                    'selling_price_per_mt': selling_price,
                    'labour_expenses': labour,
                    'logistics_per_mt': logistics,
                    'hydra_charges': hydra,
                    'commission_per_mt': commission,
                    'other_expenses': other
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
            lot_list = df_check['lot_no'].tolist()
            lot_to_delete = st.selectbox("Select Lot to Delete", lot_list)
            if st.button("Delete Lot", use_container_width=True):
                delete_lot(st.session_state.username, lot_to_delete)
                st.success(f"Lot {lot_to_delete} deleted!")
                st.rerun()
        else:
            st.info("No lots to delete yet.")

    df = load_lots(st.session_state.username)

    if df.empty:
        st.info("No data yet. Add your first lot from the sidebar!")
    else:
        df = calculate_metrics(df)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total Lots", len(df))
        col2.metric("Total Quantity", f"{df['quantity_mt'].sum()} mt")
        col3.metric("Total Revenue", f"Rs {df['total_sale_value'].sum():,.0f}")
        col4.metric("Total Net Profit", f"Rs {df['net_profit'].sum():,.0f}")
        col5.metric("Avg Margin", f"{df['profit_margin_pct'].mean():.2f}%")

        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            fig1 = px.bar(df, x='lot_no', y='net_profit', color='area',
                          title='Net Profit per Lot',
                          labels={'lot_no': 'Lot No', 'net_profit': 'Net Profit (Rs)'},
                          text='net_profit',
                          color_discrete_sequence=px.colors.qualitative.Set2)
            fig1.update_traces(texttemplate='Rs %{text:,.0f}', textposition='outside')
            fig1.update_layout(height=400)
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=df['date_of_purchase'], y=df['purchase_price_per_mt'],
                                      mode='lines+markers', name='Purchase Price (Rs/mt)',
                                      line=dict(color='red', width=2)))
            fig2.add_trace(go.Scatter(x=df['date_of_purchase'], y=df['selling_price_per_mt'],
                                      mode='lines+markers', name='Selling Price (Rs/mt)',
                                      line=dict(color='green', width=2)))
            fig2.update_layout(title='Purchase vs Selling Price Trend (Rs/mt)',
                               xaxis_title='Date', yaxis_title='Price (Rs/mt)', height=400)
            st.plotly_chart(fig2, use_container_width=True)

        col3, col4 = st.columns(2)
        with col3:
            expense_labels = ['Labour', 'Logistics', 'Hydra', 'Commission', 'Other']
            expense_totals = [
                df['labour_expenses'].sum(),
                df['total_logistics'].sum(),
                df['hydra_charges'].sum(),
                df['total_commission'].sum(),
                df['other_expenses'].sum()
            ]
            fig3 = px.pie(names=expense_labels, values=expense_totals,
                          title='Total Expense Breakdown',
                          color_discrete_sequence=px.colors.qualitative.Pastel, hole=0.3)
            fig3.update_traces(textinfo='percent+label')
            fig3.update_layout(height=400)
            st.plotly_chart(fig3, use_container_width=True)

        with col4:
            fig4 = px.line(df, x='date_of_purchase', y='profit_margin_pct', markers=True,
                           title='Profit Margin % over Time',
                           labels={'date_of_purchase': 'Date', 'profit_margin_pct': 'Margin (%)'},
                           color_discrete_sequence=['#6366f1'])
            fig4.add_hline(y=df['profit_margin_pct'].mean(), line_dash='dash',
                           line_color='orange',
                           annotation_text=f"Avg: {df['profit_margin_pct'].mean():.2f}%")
            fig4.update_layout(height=400)
            st.plotly_chart(fig4, use_container_width=True)

        col5, col6 = st.columns(2)
        with col5:
            fig5 = px.scatter(df, x='quantity_mt', y='net_profit',
                              size='profit_margin_pct', color='area',
                              hover_data=['lot_no', 'profit_per_mt'],
                              title='Quantity (mt) vs Net Profit',
                              labels={'quantity_mt': 'Quantity (mt)', 'net_profit': 'Net Profit (Rs)'},
                              color_discrete_sequence=px.colors.qualitative.Bold)
            fig5.update_layout(height=400)
            st.plotly_chart(fig5, use_container_width=True)

        with col6:
            fig6 = go.Figure()
            expense_data = {
                'Labour': df['labour_expenses'],
                'Logistics': df['total_logistics'],
                'Hydra': df['hydra_charges'],
                'Commission': df['total_commission'],
                'Other': df['other_expenses']
            }
            colors = ['#f87171', '#fb923c', '#fbbf24', '#34d399', '#60a5fa']
            for (label, values), color in zip(expense_data.items(), colors):
                fig6.add_trace(go.Bar(name=label, x=df['lot_no'], y=values, marker_color=color))
            fig6.update_layout(barmode='stack', title='Expense Breakdown per Lot',
                               xaxis_title='Lot No', yaxis_title='Expenses (Rs)', height=400)
            st.plotly_chart(fig6, use_container_width=True)

        st.divider()
        st.subheader("Complete Deal Summary")
        summary = df[[
            'lot_no', 'area', 'quantity_mt',
            'purchase_price_per_mt', 'selling_price_per_mt',
            'total_purchase_value', 'total_sale_value',
            'labour_expenses', 'total_logistics', 'hydra_charges',
            'total_commission', 'other_expenses',
            'total_expenses', 'gross_profit', 'net_profit',
            'profit_margin_pct', 'profit_per_mt', 'deal_duration_days'
        ]].copy()
        summary.columns = [
            'Lot No', 'Area', 'Qty (mt)',
            'Purchase Rs/mt', 'Selling Rs/mt',
            'Purchase Value (Rs)', 'Sale Value (Rs)',
            'Labour (Rs)', 'Logistics (Rs)', 'Hydra (Rs)',
            'Commission (Rs)', 'Other (Rs)',
            'Total Expenses (Rs)', 'Gross Profit (Rs)', 'Net Profit (Rs)',
            'Margin %', 'Profit/mt (Rs)', 'Deal Days'
        ]
        st.dataframe(summary, use_container_width=True)

        st.divider()
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(label="Download Data as CSV",
                           data=csv, file_name='scrap_data_export.csv', mime='text/csv')