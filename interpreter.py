'''
This script will summarize sensor data into activity data using a local llm.
'''

import duckdb
import io
from ollama import chat
import pandas as pd


model = 'qwen3:8b'

def get_activities(data, step = 1):
    print(f'Summarizing step {step}...')
    format = '|Activity|Start Time|End Time|Duration|Notes|'
    system_prompt = f"You are a data scientist tasked with interpreting home sensor data from sensors placed around a subject's house. Provide your answers in the following tabular format for easy parsing: {format}"
    # Chat with a system prompt
    response = chat(model, 
                    messages=[
                        {'role': 'system', 'content': system_prompt},
                        {'role': 'user', 'content': f'Create a summary of activities within the following csv time window. Include in your summary your best guess as to what might be happening: {data}'}
                    ])
    # We want to grab the table from the output
    table_str = "\n".join([line for line in response.message.content.split('\n') if line.strip().startswith('|')])

    # Read into a DataFrame
    df = pd.read_csv(io.StringIO(table_str), sep="|", skipinitialspace=True).dropna(axis=1, how='all')

    # Clean column names (removing whitespace)
    df.columns = df.columns.str.strip()

    # Remove '---' from rows (part of the way the llm generates tables)
    df = df[~df.isin(['---']).any(axis=1)]

    print(df)
    # Return the activities summary
    return(df.to_dict('records'))

data_location = './data/sensordata.parquet'

db = duckdb.connect()
db.execute(f"CREATE VIEW subject1 AS SELECT * FROM read_parquet('{data_location}')")


# Set timestep boundaries
start = 0
stop = 500
step = 250

# Initialize the activities dictionary
activities = []

query = "SELECT * FROM subject1 LIMIT ? OFFSET ?"

for offset in range(start, stop, step):
    timestep = db.execute(query, [step, offset]).df()
    activities.extend(get_activities(timestep, round(offset / step) + 1))

# Export output to csv
df = pd.DataFrame.from_dict(activities)
print(df)
df.to_csv('./data/output.csv')
