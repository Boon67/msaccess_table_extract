import streamlit as st
import os
import importlib
import inspect
import lib.utils.session #Helper for session state

st.set_page_config(layout="wide")   

def main():

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    lib.utils.session.initSnowflake(ROOT_DIR + "secrets/configuration.toml")
    st.title("Snowflake Management Tools")

    # Get a list of all files in the pages directory
    pages_dir = "pages"
    page_files = [f for f in os.listdir(pages_dir) if f.endswith(".py") and f != "__init__.py"]
    pages=[]
    for page_file in page_files:
        pages.append(st.Page(f"./{pages_dir}/{page_file}"))

    pg = st.navigation(pages)
    pg.run()



if __name__ == "__main__":
    main()



