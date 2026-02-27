import os
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
OUT_DIR = r"C:\Users\lelee\OneDrive\Documentos\ALX\TRAE ALX\performance"
os.makedirs(OUT_DIR, exist_ok=True)
st.set_page_config(page_title="Upload de Excel", layout="centered")
st.title("Upload de Excel")
uploaded = st.file_uploader("Selecione o arquivo Excel ou CSV", type=["xlsx", "csv"])
df = None
if uploaded:
    name = os.path.basename(uploaded.name)
    if name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded)
    else:
        df = pd.read_excel(uploaded)
    df.columns = [str(c).strip() for c in df.columns]
    st.success("Arquivo carregado")
    st.dataframe(df.head(50))
    d1 = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    base = f"upload_{d1}_{datetime.now().strftime('%H%M%S')}"
    save_xlsx = os.path.join(OUT_DIR, f"{base}.xlsx")
    save_csv = os.path.join(OUT_DIR, f"{base}.csv")
    colunas_pix = ["id_pagamento", "nome", "documento", "chave_pix", "valor", "descricao"]
    falta = [c for c in colunas_pix if c not in [x.lower().replace(" ", "_") for x in df.columns]]
    if st.button("Salvar como XLSX"):
        with pd.ExcelWriter(save_xlsx, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="dados")
        st.info(f"Salvo em: {save_xlsx}")
    if st.button("Salvar como CSV"):
        df.to_csv(save_csv, index=False, encoding="utf-8")
        st.info(f"Salvo em: {save_csv}")
    if falta:
        st.warning("Colunas esperadas para PIX ausentes: " + ", ".join(falta))
