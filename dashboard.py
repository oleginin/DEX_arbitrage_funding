# -*- coding: utf-8 -*-
import streamlit as st
import sqlite3
import pandas as pd
import time
import os

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ НАЛАШТУВАННЯ
# ═══════════════════════════════════════════════════════════════════════════

REFRESH_SECONDS = 15

st.set_page_config(
    page_title="Arbitrage Scanner",
    page_icon="🚀",
    layout="wide",
)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Database', 'arbitrage_dashboard.db')


def load_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        query = "SELECT * FROM live_opportunities"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 ОСНОВНИЙ ДАШБОРД
# ═══════════════════════════════════════════════════════════════════════════

# Генерація посилань
def get_trade_url(exchange, token):
    ex = exchange.lower().strip()
    token = token.upper().strip()

    if 'lighter' in ex:
        return f"https://app.Lighter.xyz/trade/{token}/?referral=118787PQ"
    elif 'paradex' in ex:
        return f"https://app.Paradex.trade/trade/{token}-USD-PERP"
    elif 'variational' in ex or 'omni' in ex:
        return f"https://omni.Variational.io/perpetual/{token}"
    elif 'backpack' in ex:
        return f"https://Backpack.exchange/trade/{token}_USD_PERP"
    elif 'extended' in ex:
        return f"https://app.Extended.exchange/perp/{token}-USD"
    else:
        return f"https://www.google.com/search?q={exchange.capitalize()}+{token}+perp"


st.title("🚀 Live Arbitrage Dashboard")

df = load_data()

with st.container(border=True):
    st.markdown("### 🛠 Налаштування")

    col1, col2, col3, col4 = st.columns([1, 2, 2, 1])

    with col1:
        min_spread = st.number_input(
            "📉 Мін. спред (%)",
            min_value=-100.0,
            value=-100.0,
            step=0.1
        )

    with col2:
        all_tokens = sorted(df['token'].unique()) if not df.empty else []
        search_token = st.multiselect("Coin", all_tokens, placeholder="Всі")

    with col3:
        all_exchanges = sorted(
            set(df['buy_exchange'].unique()) | set(df['sell_exchange'].unique())) if not df.empty else []
        selected_exchanges = st.multiselect("Exchanges", all_exchanges, placeholder="Всі")

    with col4:
        st.markdown("<br>", unsafe_allow_html=True)
        auto_refresh = st.toggle("🔄 Авто-оновлення", value=True)
        timer_placeholder = st.empty()
        if st.button("Оновити"):
            st.rerun()

if not df.empty:
    df_filtered = df[df['spread_pct'] >= min_spread].copy()

    if search_token:
        df_filtered = df_filtered[df_filtered['token'].isin(search_token)]

    if selected_exchanges:
        mask = df_filtered['buy_exchange'].isin(selected_exchanges) | df_filtered['sell_exchange'].isin(
            selected_exchanges)
        df_filtered = df_filtered[mask]

    df_filtered = df_filtered.sort_values(by='spread_pct', ascending=False)

    # Генеруємо посилання
    df_filtered['buy_link'] = df_filtered.apply(lambda row: get_trade_url(row['buy_exchange'], row['token']), axis=1)
    df_filtered['sell_link'] = df_filtered.apply(lambda row: get_trade_url(row['sell_exchange'], row['token']), axis=1)

    m1, m2, m3 = st.columns(3)
    m1.metric("Маршрутів", len(df_filtered))
    if not df_filtered.empty:
        best_spread = df_filtered.iloc[0]['spread_pct']
        best_pair = f"{df_filtered.iloc[0]['token']} ({df_filtered.iloc[0]['route']})"
        m2.metric("Топ спред", f"{best_spread:.2f}%")
        m3.metric("Топ пара", best_pair)

    # --- НАЛАШТУВАННЯ КОЛОНОК (ОНОВЛЕНО) ---
    display_cols = [
        'token',
        'buy_link', 'sell_link',
        'spread_pct',
        # 🔥 Нові колонки фандінгу
        'buy_funding_rate', 'buy_funding_freq',
        'sell_funding_rate', 'sell_funding_freq',
        'spread_min_24h', 'spread_max_24h',
        'buy_price', 'sell_price'
    ]

    clean_name_regex = r"https?://(?:www\.|app\.|omni\.)?(\w+)"

    column_config = {
        "token": st.column_config.TextColumn("Token", width="small"),

        "buy_link": st.column_config.LinkColumn(
            "Buy Route",
            display_text=clean_name_regex,
            width="medium"
        ),

        "sell_link": st.column_config.LinkColumn(
            "Sell Route",
            display_text=clean_name_regex,
            width="medium"
        ),

        "spread_pct": st.column_config.NumberColumn("Spread", format="%.2f %%"),

        # 🔥 Налаштування відображення нових колонок
        "buy_funding_rate": st.column_config.NumberColumn("Buy Fund", format="%.4f %%"),
        "buy_funding_freq": st.column_config.NumberColumn("Buy Freq (h)"),
        "sell_funding_rate": st.column_config.NumberColumn("Sell Fund", format="%.4f %%"),
        "sell_funding_freq": st.column_config.NumberColumn("Sell Freq (h)"),

        "spread_min_24h": st.column_config.NumberColumn("Min 24h", format="%.2f %%"),
        "spread_max_24h": st.column_config.NumberColumn("Max 24h", format="%.2f %%"),
        "buy_price": st.column_config.NumberColumn("Buy Price", format="%.4f"),
        "sell_price": st.column_config.NumberColumn("Sell Price", format="%.4f"),
    }


    def highlight_spread(val):
        if val > 0.5:
            return 'background-color: #d4edda; color: black;'
        elif val > 0:
            return 'background-color: #fff3cd; color: black;'
        else:
            return 'background-color: #f8d7da; color: black;'


    st.dataframe(
        df_filtered[display_cols].style.map(highlight_spread, subset=['spread_pct']),
        width="stretch",
        height=800,
        column_config=column_config,
        hide_index=True
    )

else:
    st.info("⏳ Очікування даних...")

if auto_refresh:
    for i in range(REFRESH_SECONDS, 0, -1):
        timer_placeholder.markdown(f"⏳ Оновлення: **{i}** с")
        time.sleep(1)
    st.rerun()
else:
    timer_placeholder.markdown("⏸️ **Пауза**")