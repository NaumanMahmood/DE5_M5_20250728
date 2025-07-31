import streamlit as st
from datetime import datetime

st.title("How Old Will You Be in 2063?")
st.write("Enter your details below:")

# Input fields
name = st.text_input("Your Name")
dob = st.date_input("Date of Birth")

if name and dob:
    age_in_2063 = 2063 - dob.year
    st.success(f"Hello {name}! You will be **{age_in_2063} years old** in the year 2063.")