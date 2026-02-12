import streamlit as st
from fuzzy.classifier import get_response

st.set_page_config(
    page_title="AI Profile Classifier",
    page_icon="🧬"
)

st.title("AI Profile Classifier 🧬")
st.markdown("""
This AI profile classifier will help you evaluate your profile strength.
""")
st.divider()

with st.form("my_form"):
    username = st.text_input(
        "Enter Your Username 👇",
        placeholder="e.g. neal_wu",
    )
    submitted = st.form_submit_button("Submit")
    if submitted:
        expertise_level, expertise_name, recom, contest_data, submission_data, res = get_response(username)
        if submission_data:
            st.markdown(f'''Your profile score is **{float(expertise_level)}/10** and expertise level is **{expertise_name}**!\n''')
            st.markdown(f"""
                    <u><strong>Your Data</strong></u><br>
                    ```
                    • Contest Rating:   {round(contest_data[0], 2)}
                    • Hard questions:    {submission_data[2]}
                    • Medium questions:  {submission_data[1]}
                    • Easy questions:    {submission_data[0]} 
                    ```
            """, unsafe_allow_html=True)

            st.markdown(f"""<u><strong>Recommendation</strong></u><br>""", unsafe_allow_html=True)
            st.markdown(f"""{recom}""")

        else:
            st.markdown(
                f'''{res}\n''')


