import streamlit as st

def label_grade_formatter(s):
    return s.split("_", maxsplit=1)[0].capitalize()


def local_css(file_name):
    with open(file_name) as f:
        st.markdown('<style>{}</style>'.format(f.read()), unsafe_allow_html=True)