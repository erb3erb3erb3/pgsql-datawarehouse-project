# Data Catalog for Gold Layer

## Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of **dimension tables** and **fact tables** for specific business metrics.

---

### 1. **gold.dim_customers**
- **Purpose:** Stores customer demographic and geographic details.
- **Columns:**

| Column Name      | Data Type     | Description                                                                                              |
|------------------|---------------|----------------------------------------------------------------------------------------------------------|
| customer_key     | INT           | Surrogate key generated to uniquely identify each customer record in the dimension table.                |
| customer_id      | INT           | Unique numerical identifier assigned to each customer, used for internal tracking and referencing.       |
| customer_number  | NVARCHAR(50)  | Alphanumeric identifier representing the customer, often used for customer facing applications.          |
| first_name       | NVARCHAR(50)  | The customer's first name.                                                                               |
| last_name        | NVARCHAR(50)  | The customer's last name.                                                                                |
| country          | NVARCHAR(50)  | The country of residence for the customer (e.g., 'Australia').                                           |
| marital_status   | NVARCHAR(50)  | The marital status of the customer (e.g., 'Married', 'Single', 'N/A').                                   |
| gender           | NVARCHAR(50)  | The gender of the customer (e.g., 'Male', 'Female', 'N/A').                                              |
| birthdate        | DATE          | The date of birth of the customer. Formatted as YYYY-MM-DD (e.g., 1999-12-31).                           |
| create_date      | DATE          | The date when the customer record was created in the system. Formatted as YYYY-MM-DD (e.g., 1999-12-31). |

---

### 2. **gold.dim_products**
- **Purpose:** Provides information about the products and their categorical classifications.
- **Columns:**

| Column Name         | Data Type     | Description                                                                                                                    |
|---------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------|
| product_key         | INT           | Surrogate key generated to uniquely identify each product record in the product dimension table.                               |
| product_id          | INT           | A unique identifier assigned to the product for internal tracking and referencing.                                             |
| product_number      | NVARCHAR(50)  | A structured alphanumeric code representing the product, often used for categorization or inventory.                           |
| product_name        | NVARCHAR(50)  | Descriptive name of the product.                                                                                               |
| category_id         | NVARCHAR(50)  | A unique identifier for the product's category.                                                                                |
| category            | NVARCHAR(50)  | The broad classification of the product (e.g., 'Bikes', 'Components').                                                         |
| subcategory         | NVARCHAR(50)  | A more detailed classification of the product within the category, such as product type. e.g. ('Mountain Bike', 'Chains')      |
| maintenance_required| NVARCHAR(50)  | Indicates whether the product requires maintenance (e.g., 'Yes', 'No').                                                        |
| cost                | INT           | The cost or base price of the product, measured in USD.                                                                        |
| product_line        | NVARCHAR(50)  | The product line to which the product belongs (e.g., Road, Mountain).                                                          |
| start_date          | DATE          | The date when the product became available for sale or use. Formatted as YYYY-MM-DD (e.g., 1999-12-31).                        |

---

### 3. **gold.fact_sales**
- **Purpose:** Stores transactional sales data for analytical purposes.
- **Columns:**

| Column Name     | Data Type     | Description                                                                                         |
|-----------------|---------------|-----------------------------------------------------------------------------------------------------|
| order_number    | NVARCHAR(50)  | A unique alphanumeric identifier for each sales order.                                              |
| product_key     | INT           | Surrogate key linking the sales order to the product dimension table.                               |
| customer_key    | INT           | Surrogate key linking the sales order to the customer dimension table.                              |
| order_date      | DATE          | The date when the order was placed. Formatted as YYYY-MM-DD (e.g., 1999-12-31).                     |
| shipping_date   | DATE          | The date when the order was shipped to the customer. Formatted as YYYY-MM-DD (e.g., 1999-12-31).    |
| due_date        | DATE          | The date when the order payment was due. Formatted as YYYY-MM-DD (e.g., 1999-12-31).                |
| sales_amount    | INT           | The total monetary value of the sale in USD.                                                        |
| quantity        | INT           | The number of units of product ordered.                                                             |
| price           | INT           | The price per unit of the product in USD.                                                           |
