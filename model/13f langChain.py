from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI

# MySQL database URI
mysql_uri = 'password'
open_api_key = 'api_key'

# Creating a database instance
db = SQLDatabase.from_uri(mysql_uri)

# Function to get the database schema with detailed information
def get_schema(db):
    schema = db.get_table_info()
    print(schema)
    detailed_schema = {}
    for table, info in schema.items():
        detailed_schema[table] = {
            'columns': info['columns'],
            'sample_data': db.run(f"SELECT * FROM {table} LIMIT 5")
        }
    return detailed_schema

# Function to run a query on the database
def run_query(query):
    print(query)
    return db.run(query)

# Prompt template for generating SQL query with examples and more detailed instructions
sql_query_template = """Based on the table schema below, write a precise SQL query that would answer the user's question:
Schema:
{schema}

Question: {question}

Remember to consider table relationships and use JOINs if necessary. Here are some examples of similar queries:
Example 1:
Schema: {schema_example_1}
Question: {question_example_1}
SQL Query: {query_example_1}

Example 2:
Schema: {schema_example_2}
Question: {question_example_2}
SQL Query: {query_example_2}

Now, write the SQL query for the given question.
SQL Query:"""
prompt = ChatPromptTemplate.from_template(sql_query_template)

# Language model instance
llm = ChatOpenAI(api_key=open_api_key)

# SQL generation chain with validation
sql_chain = (
    RunnablePassthrough.assign(schema=lambda vars: get_schema(vars['db']))
    | prompt
    | llm.bind(stop=["\nSQLResult:"])
    | StrOutputParser()
)

# Function to validate SQL query
def validate_query(query):
    try:
        # Check if the query is valid by running an explain statement
        db.run(f"EXPLAIN {query}")
        return query
    except Exception as e:
        raise ValueError(f"Invalid SQL query: {query}. Error: {e}")

# Prompt template for generating natural language response with more context
response_template = """Based on the table schema below, the question, the SQL query, and the SQL response, write a natural language response:
Schema:
{schema}

Question: {question}

SQL Query: {query}

SQL Response: {response}

Make sure the response is clear and answers the user's question fully.
"""
prompt_response = ChatPromptTemplate.from_template(response_template)

# Full processing chain with validation step
full_chain = (
    RunnablePassthrough.assign(query=sql_chain).assign(
        schema=lambda vars: get_schema(vars['db']),
        query=validate_query,
        response=lambda vars: run_query(vars["query"]),
    )
    | prompt_response
    | llm.bind(stop=["\nEndResponse"])
    | StrOutputParser()
)

user_question = "Give me the names of the top 3 holdings of Meta"
input_vars = {"question": user_question, "db": db}

# Invoke the processing chain
result = full_chain.invoke(input_vars)

# Print the result
print("Final result:", result)

