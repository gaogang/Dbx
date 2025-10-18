# Databricks notebook source
# MAGIC %md
# MAGIC # A study in Sankey waterfall chart generation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Installation
# MAGIC
# MAGIC First, let's install the plotly and restart the Python kernel:

# COMMAND ----------

# MAGIC %pip install plotly
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Draw a simple chart (using the assistant)
# MAGIC
# MAGIC Now, we'll draw a simple bar chart using randomly generated sample data (and AI assistant)
# MAGIC
# MAGIC **Prompt** - _draw a vertical bar chart using randomly generate data in pastel color_

# COMMAND ----------

import plotly.graph_objects as go
import random

# Generate random data
categories = [f'Category {i+1}' for i in range(6)]
values = [random.randint(10, 100) for _ in categories]

# Pastel color palette
pastel_colors = [
    'rgba(255,179,186,0.7)', 'rgba(255,223,186,0.7)', 'rgba(255,255,186,0.7)',
    'rgba(186,255,201,0.7)', 'rgba(186,225,255,0.7)', 'rgba(218,186,255,0.7)'
]

fig = go.Figure(data=[
    go.Bar(
        x=categories,
        y=values,
        marker_color=pastel_colors
    )
])

fig.update_layout(
    title="Vertical Bar Chart with Random Data (Pastel Colors)",
    xaxis_title="Category",
    yaxis_title="Value"
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a simple Sankey chart from testing data (using AI)
# MAGIC
# MAGIC Prompt - _draw a simple sankey chart using plotly from randomly generated testing data in pastel color_
# MAGIC
# MAGIC > **_NOTE:_**  The code below are AI generated.

# COMMAND ----------

import plotly.graph_objects as go
import random

# Sample data for two-layer sankey
sources = [0, 0, 1, 1]
targets = [2, 3, 2, 3]
values = [random.randint(2, 10) for _ in range(4)]

# Pastel color palette for links
pastel_colors = [
    'rgba(255,179,186,0.7)', 'rgba(255,223,186,0.7)',
    'rgba(255,255,186,0.7)', 'rgba(186,255,201,0.7)'
]
link_colors = [pastel_colors[i % len(pastel_colors)] for i in range(len(values))]

fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=["A", "B", "C", "D"],
        color=['rgba(255,179,186,0.7)', 'rgba(255,223,186,0.7)', 'rgba(186,255,201,0.7)', 'rgba(186,225,255,0.7)']
    ),
    link=dict(
        source=sources,
        target=targets,
        value=values,
        color=link_colors
    )
)])

fig.update_layout(title_text="Simple Two Layer Sankey Diagram (Pastel Colors)", font_size=10)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Sankey chart from NHS England patient care data
# MAGIC
# MAGIC - Download the [latest NHS England Patient Care Activity Data](https://digital.nhs.uk/data-and-information/publications/statistical/hospital-admitted-patient-care-activity/2024-25)
# MAGIC - Upload the diagnosis data to Worksapce
# MAGIC Path: /Workspace/Users/username/Data/NHS England/Admitted Patient Care Activity/Diagnosis
# MAGIC - Load data to a delta table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Prompts - 
# MAGIC
# MAGIC Create a sankey diagram using plotly and data (2025 only) from data table `depq-nhs`.patient_care.diagnosis_lv2 with the following instruction:
# MAGIC
# MAGIC Common cancer code:
# MAGIC
# MAGIC Lung Cancer: C33-C34 
# MAGIC Colorectal Cancer: C18-C21 
# MAGIC Breast Cancer: C50
# MAGIC Prostate Cancer: C61
# MAGIC Cervical Cancer: C53
# MAGIC Melanoma: C43
# MAGIC Stomach Cancer: C16
# MAGIC Pancreatic Cancer: C25
# MAGIC Ovarian Cancer: C56 
# MAGIC
# MAGIC Sankey diagram: 2 levels
# MAGIC
# MAGIC Source: Sexuality which can either male or female
# MAGIC
# MAGIC level 1
# MAGIC target: common cancer type
# MAGIC value: number of finished admission
# MAGIC level 2
# MAGIC target: FAE data which are Emergency, Waiting list, Planned or Other_
# MAGIC
# MAGIC > **_NOTE:_**  The code below are AI generated.
# MAGIC

# COMMAND ----------

import plotly.graph_objects as go

# Define cancer code mapping
cancer_map = {
    'Lung Cancer': ['C33', 'C34'],
    'Colorectal Cancer': ['C18', 'C19', 'C20', 'C21'],
    'Breast Cancer': ['C50'],
    'Prostate Cancer': ['C61'],
    'Cervical Cancer': ['C53'],
    'Melanoma': ['C43'],
    'Stomach Cancer': ['C16'],
    'Pancreatic Cancer': ['C25'],
    'Ovarian Cancer': ['C56']
}

# Prepare SQL WHERE clause for cancer codes
cancer_code_filters = []
for codes in cancer_map.values():
    for code in codes:
        if '-' in code:
            start, end = code.split('-')
            cancer_code_filters.append(f"(Code >= '{start}' AND Code <= '{end}')")
        else:
            cancer_code_filters.append(f"(Code LIKE '{code}%')")
cancer_code_filter_sql = " OR ".join(cancer_code_filters)

# Query and transform data for 2025 only
df = spark.sql(f"""
SELECT
  CASE
    WHEN Code LIKE 'C33%' OR Code LIKE 'C34%' THEN 'Lung Cancer'
    WHEN Code LIKE 'C18%' OR Code LIKE 'C19%' OR Code LIKE 'C20%' OR Code LIKE 'C21%' THEN 'Colorectal Cancer'
    WHEN Code LIKE 'C50%' THEN 'Breast Cancer'
    WHEN Code LIKE 'C61%' THEN 'Prostate Cancer'
    WHEN Code LIKE 'C53%' THEN 'Cervical Cancer'
    WHEN Code LIKE 'C43%' THEN 'Melanoma'
    WHEN Code LIKE 'C16%' THEN 'Stomach Cancer'
    WHEN Code LIKE 'C25%' THEN 'Pancreatic Cancer'
    WHEN Code LIKE 'C56%' THEN 'Ovarian Cancer'
    ELSE NULL
  END AS cancer_type,
  SUM(Male) AS Male,
  SUM(Female) AS Female,
  SUM(Emergency) AS Emergency,
  SUM(`Waiting List`) AS `Waiting List`,
  SUM(Planned) AS Planned,
  SUM(Other) AS Other
FROM `depq-nhs`.patient_care.diagnosis_lv2
WHERE ({cancer_code_filter_sql}) AND Year = 2025
GROUP BY cancer_type
""")

display(df)

# Convert to Pandas for easier manipulation
pdf = df.toPandas()

# Prepare sankey nodes and links
nodes = ['Male', 'Female']
cancer_types = pdf['cancer_type'].dropna().unique().tolist()
nodes += cancer_types
fae_types = ['Emergency', 'Waiting List', 'Planned', 'Other']
nodes += fae_types

node_indices = {name: i for i, name in enumerate(nodes)}

# Level 1: Gender -> Cancer Type (using Finished Admission Episodes)
links_source = []
links_target = []
links_value = []
links_color = []

pastel_colors = [
    'rgba(255,179,186,0.7)', 'rgba(255,223,186,0.7)', 'rgba(255,255,186,0.7)',
    'rgba(186,255,201,0.7)', 'rgba(186,225,255,0.7)', 'rgba(218,186,255,0.7)',
    'rgba(255,200,255,0.7)', 'rgba(200,255,255,0.7)', 'rgba(255,220,200,0.7)'
]

# Gender to Cancer Type (Finished Admission Episodes = Male + Female)
for idx, row in pdf.iterrows():
    for gender in ['Male', 'Female']:
        value = row[gender]
        if value > 0:
            links_source.append(node_indices[gender])
            links_target.append(node_indices[row['cancer_type']])
            links_value.append(value)
            links_color.append(pastel_colors[cancer_types.index(row['cancer_type']) % len(pastel_colors)])

# Cancer Type to FAE
for idx, row in pdf.iterrows():
    for fae in fae_types:
        value = row[fae]
        if value > 0:
            links_source.append(node_indices[row['cancer_type']])
            links_target.append(node_indices[fae])
            links_value.append(value)
            links_color.append(pastel_colors[cancer_types.index(row['cancer_type']) % len(pastel_colors)])

fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=nodes,
        color=pastel_colors * ((len(nodes) // len(pastel_colors)) + 1)
    ),
    link=dict(
        source=links_source,
        target=links_target,
        value=links_value,
        color=links_color
    )
)])

fig.update_layout(title_text="NHS England Patient Care: Cancer Type by Gender and Admission Type (2025)", font_size=10)
fig.show()
