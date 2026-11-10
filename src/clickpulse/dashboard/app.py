"""ClickPulse Streamlit dashboard entry point."""

import streamlit as st


def main() -> None:
    st.set_page_config(page_title="ClickPulse Analytics", layout="wide")
    st.title("ClickPulse — Clickstream Analytics")
    st.info("Dashboard components will be connected to Druid after ingestion is implemented.")


if __name__ == "__main__":
    main()
