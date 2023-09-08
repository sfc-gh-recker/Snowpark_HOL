# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
tab1, tab2= st.tabs(["Application", "Data Quality"])
# Write directly to the app
with tab1:
    
    st.title("Copy & Paste Into Table")
    st.write(
        """Select cells from Excel, paste in the 'Dataset' section, and automatically create a table"""
    )
    
    # Get the current credentials
    session = get_active_session()
    
    # Input fields in the sidebar
    dbschema = session.sql("SELECT CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SCMA").to_pandas()
    db = st.sidebar.text_input('Database', dbschema["DB"][0])
    schema = st.sidebar.text_input('Schema', dbschema["SCMA"][0])
    table = st.sidebar.text_input('New Table', f"NEW_TABLE")
    dataset = st.text_area("Dataset", height=300)
    
    buttons = st.sidebar.columns(2)
    create_table = buttons[0].button("Create/Replace Table")
    append_data = buttons[1].button('Append Data')
    
    if create_table or append_data:
        lines = dataset.split("\n")
        if append_data:
            try:
                # check if table already exists
                session.sql(f'SELECT 1 FROM "{db}"."{schema}"."{table}" LIMIT 1').collect()
            except:
                st.error("Table does not exist.")
                st.stop()
    
        for line in lines:
            cols = line.split("\t")
            if create_table:
                if line == lines[0]:
                    fields = [f'"{col}" string' for col in cols]
                    query = f'CREATE OR REPLACE TABLE "{db}"."{schema}"."{table}" ({", ".join(fields)})'
                    session.sql(query).collect()
                else:
                    fields = [f"'{col}'" for col in cols]
                    query = f"""INSERT INTO "{db}"."{schema}"."{table}" VALUES ({', '.join(fields)})"""
                    session.sql(query).collect()
            elif append_data:
                fields = [f"'{col}'" for col in cols]
                query = f"""INSERT INTO "{db}"."{schema}"."{table}" VALUES ({', '.join(fields)})"""
                session.sql(query).collect()
    
        st.success('Operation Complete!')
        st.caption(f'SELECT * FROM "{db}"."{schema}"."{table}"')
        st.write(session.table(f'"{db}"."{schema}"."{table}"').to_pandas())
    
    
        df = session.table(f'"{db}"."{schema}"."{table}"').to_pandas()
       
with tab2:
    
    df = session.table(f'"{db}"."{schema}"."{table}"').to_pandas()   

    # Data Quality Checks

    col1, col2 = st.columns(2)
    with col1:
        st.metric(label="Number of Rows", value=df.shape[0])
    with col2:
        st.metric(label="Number of Columns", value=df.shape[1])
    
    # Number of missing values
    st.write('Missing Values:')
    st.write(df.isnull().sum())
    
    # Number of duplicate rows
    st.metric(label="Duplicate Rows", value=df.duplicated().sum())
    
    # Number of unique values per column
    st.write('Unique Values:')
    st.write(df.nunique())
    
    # Statistics for numerical columns
    st.write('Statistics for numerical columns:')
    st.write(df.describe())
    
    # Number of zeros per column
    st.write('Zeros per column:')
    st.write((df == 0).astype(int).sum())
    
    # Number of blank spaces per column
    st.write('Blank spaces per column:')
    st.write((df == ' ').astype(int).sum())


