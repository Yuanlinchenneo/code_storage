## Code to generate sankey diagram via plotly
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import urllib, json


# Nodes (the entities in the flow)
labels = ["Bank Account", "Rent", "Groceries", "Utilities", "Savings", "Travel"]


# Explicitly set the x and y coordinates for each node
x_coords = [0, 0.5, 0.5, 0.5, 0.5, 1]
y_coords = [0.5, 0.9, 0.6, 0.4, 0.1, 0.5]

# Links (the flow between the nodes)
source = [0, 0, 0, 0, 1, 2, 3]
target = [1, 2, 3, 4, 5, 5, 5]
value = [400, 300, 150, 400, 200, 50, 25]

# Create the Sankey diagram
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=5,
        thickness=10,
        line=dict(color="black", width=0.5),
        label=labels,
        color=["blue", "red", "green", "purple", "orange", "yellow"],
        # Use the x and y coordinates to control placement
        x=x_coords,
        y=y_coords
    ),
    link=dict(
        source=source,
        target=target,
        value=value,
        color=['lightcoral','lightgreen','lightpink','gold','lemonchiffon','lemonchiffon','lemonchiffon','lemonchiffon']
    )
)])

# Update layout for a better look
fig.update_layout(
    title = dict(
        text = "Simple Money Flow Sankey Diagram",
        font = dict(size = 12),
        x = 0.5,
    ),
    # Add a top margin (in pixels) to prevent overlap
    margin = dict(t = 50))

# Show the diagram
fig.show()
