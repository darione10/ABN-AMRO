# EternalTeleSales Fran van Seb Group
## Overview 

This application processes telemarketing datasets for EternalTeleSales Fran van Seb Group, focusing on insights about their employees' performance in different sales areas. It leverages PySpark for data processing and analysis.

## Requirements

- **Python Version**: 3.10
- **PySpark Version**: 3.5.2
- **Testing Framework**: [Chispa](https://github.com/MrPowers/chispa)
- **No Notebooks**: Avoid using Jupyter or similar notebooks; the application runs as a standalone script.
- **No Standalone Pandas**: While PySpark can integrate with Pandas, it should not be used as a standalone library.
- **Version Control**: The project is stored in a private GitHub repository. Only relevant files are committed, following the GitHub flow for branch management.
- **Logging**: Uses logging for application status instead of print statements.

## Datasets
- The file **dataset_one.csv** has information about the area of expertise of an employee and the number of calls that were made and also calls that resulted in a sale.
- The file **dataset_two.csv** has personal and sales information, like **name**, **address** and **sales_amount**.
- The file **dataset_three.csv** has data about the sales made, which company the call was made to where the company is located, the product and quantity sold. The field **caller_id** matches the ids of the other two datasets.

## Outputs

The application produces several outputs as specified:

### Output #1 - IT Data
- Joins datasets, filters for the IT department, sorts by sales amount, and saves the top 100 records.
- **Output Directory**: `it_data`

### Output #2 - Marketing Address Information
- Extracts addresses and postal codes for employees in the Marketing department.
- **Output Directory**: `marketing_address_info`

### Output #3 - Department Breakdown
- Provides a breakdown of total sales by department along with the percentage of successful calls.
- **Output Directory**: `department_breakdown`

### Bonus Outputs

#### Output #4 - Top 3 Best Performers per Department
- Identifies the top 3 performers in each department with a success rate above 75%.
- **Output Directory**: `top_3`

#### Output #5 - Top 3 Most Sold Products per Department in the Netherlands
- **Output Directory**: `top_3_most_sold_per_department_netherlands`

#### Output #6 - Best Overall Salesperson per Country
- **Output Directory**: `best_salesperson`

### Extra Insights
- **Extra Insight One**: [Description of insight]
- **Output Directory**: `extra_insight_one`
- **Extra Insight Two**: [Description of insight]
- **Output Directory**: `extra_insight_two`

## Project Structure
The project is organized as follows:

**main.py**: The main Python script to execute data processing.

**app/utils.py**: Includes generic functions used.

**test_functions.py**: Includes the pytest functions to validate that all used under `utils.py` work properly

**.github/workflows/build_package.yaml**: Contains the GitHub Actions description for the automated build pipeline.

**datasets/**: The directory where the input files are stored for this assignment.

**requirements.txt**: A file listing project dependencies.

**exercise.md**: The file describing the assignment.

**application.log**: Logging from the application

**README.md**: This documentation file.