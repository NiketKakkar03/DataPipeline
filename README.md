# Multithreaded CSV-to-MySQL Data Pipeline
This project is a prototype implementation of the data ingestion pipeline I developed during my HeadStarter AI Fellowship. It simulates the real-time ingestion of daily IoT-like records, cleans and validates them, and writes the results into a MySQL database hosted locally via XAMPP.

 Project Structure
├── daily_dump.csv       # Input CSV with columns: id, value, timestamp
├── .env                 # Contains DB credentials (host, user, password, etc.)
├── pipeline.py          # Main pipeline logic


Setup and Usage
1. Set up XAMPP and start MySQL.
2. Create a .env file in the root
3. Install dependencies:
    pip install pandas python-dotenv mysql-connector-python
4. Run the python file
