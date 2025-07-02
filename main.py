jobsss = ['data+engineer', 'data', 'sql', 'data+scientist',
            'data+analyst','data+architect','cloud+data',
            'big+data', 'etl', 'analytics+engineer','database']
import time

def get_data(job_name):
    import random
    import requests
    from bs4 import BeautifulSoup
    from fake_useragent import UserAgent
    import pandas as pd
    import os
    import psycopg2
    import pandas as pd
    from datetime import datetime, timedelta
    import re


    title = job_name

    work_model = {'1': 'On-site', '2': 'Remote', '3': 'Hybrid'}
    job_ids = set()
    job_work_model = {}

    for n_results in range(0,21,10):
        id_WM = random.choice(list(work_model.keys()))
        list_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={title}&location=United%20States&geoId=103644278&f_WT={id_WM}&sortBy=DD&start={n_results}'

        
        ua = UserAgent()
        userAgent = ua.random
        headers = {'User-Agent': userAgent}
        response = requests.get(list_url, headers = headers)

        list_data = response.text
        list_soup = BeautifulSoup(list_data, 'html.parser')
        job_cards = list_soup.find_all('div', class_='base-card')

        for card in job_cards:
            if 'data-entity-urn' in card.attrs:
                job_id = card['data-entity-urn'].split(':')[3]
                job_ids.add(job_id)
                job_work_model[job_id] = work_model[id_WM]

    job_ids = list(job_ids)
    job_list = []

    for job_id in job_ids:
        ua = UserAgent()
        userAgent = ua.random
        headers = {'User-Agent': userAgent}
        job_url = f'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}?_l=en_US'
        job_response = requests.get(job_url, headers = headers)
        # print(job_response.status_code)

        if job_response.status_code == 200:
            job_post = {}
            job_soup = BeautifulSoup(job_response.text, 'html.parser')

            job_post['job_id'] = job_id

            try:
                job_post['job_title'] = job_soup.find('h2', {'class': 'top-card-layout__title'}).text.strip()
            except:
                job_post['job_title'] = None

            try:
                job_post['company_name'] = job_soup.find('a', {'class': 'topcard__org-name-link'}).text.strip()
            except:
                job_post['company_name'] = None

            job_post['work_model'] = job_work_model.get(job_id, None)

            try:
                job_post['location'] = job_soup.find('span', {'class': 'topcard__flavor topcard__flavor--bullet'}).text.strip()
            except:
                job_post['location'] = None   

            try:
                job_post['time_posted'] = job_soup.find('span', {'class': 'posted-time-ago__text'}).text.strip()
            except:
                job_post['time_posted'] = None

            try:
                job_post['num_applicants'] = job_soup.find('span', {'class': 'num-applicants__caption'}).text.strip()
            except:
                job_post['num_applicants'] = None
            
            job_criteria = {}
            for item in job_soup.find_all('li', class_='description__job-criteria-item'):
                try:
                    label = item.find('h3', class_='description__job-criteria-subheader').text.strip()
                    value = item.find('span', class_='description__job-criteria-text').text.strip()
                    job_criteria[label] = value
                except:
                    continue

            job_post['xp_level'] = job_criteria.get(list(job_criteria.keys())[0], None)
            job_post['job_type'] = job_criteria.get(list(job_criteria.keys())[1], None)
            job_post['job_sectors'] = job_criteria.get(list(job_criteria.keys())[3], None)

            try:
                job_post['job_description'] = job_soup.find('div', {'class': 'show-more-less-html__markup'}).get_text(separator="\n", strip=True)
            except:
                job_post['job_description'] = None

            job_list.append(job_post)


    jobs_df = pd.DataFrame(job_list)

    key_tools = [

        # 🧑‍💻 Programming Languages
        'Python', 'R', 'Java', 'Scala', 'SQL', 'NoSQL', 'Julia', 'TypeScript', 'JavaScript', 'Go', 'C++',

        # 📊 Data Analysis & Manipulation
        'Pandas', 'NumPy', 'Dask', 'Polars', 'OpenRefine', 'Tidyverse', 'DataFrames', 'Vaex', 'Koalas', 'Pyspark'

        # 🤖 Machine Learning & AI
        'Scikit-learn', 'TensorFlow', 'PyTorch', 'Keras', 'XGBoost', 'LightGBM', 'CatBoost',
        'FastAI', 'MLlib', 'H2O.ai', 'AutoML', 'DeepLearning4j', 'Optuna', 'MLflow', 'Hugging Face', 'ONNX',

        # 📈 Data Visualization
        'Matplotlib', 'Seaborn', 'Plotly', 'Bokeh', 'Altair', 'Dash',
        'Tableau', 'Power BI', 'Looker', 'QuickSight', 'Google Data Studio', 'Superset', 'QlikView', 'MicroStrategy',

        # ☁️ Cloud Platforms
        'AWS', 'Amazon S3', 'Athena', 'Redshift', 'Glue', 'Lambda',
        'GCP', 'BigQuery', 'Dataflow', 'Google Cloud Storage', 'Vertex AI',
        'Azure', 'Azure Synapse', 'Azure Data Lake', 'Azure ML', 'Databricks', 'Data Factory', 'Blob Storage'

        # 🧱 Databases & Warehousing
        'PostgreSQL', 'MySQL', 'SQL Server', 'Oracle', 'SQLite',
        'Snowflake', 'Databricks', 'ClickHouse', 'Vertica', 'Greenplum',
        'MongoDB', 'Cassandra', 'Elasticsearch', 'InfluxDB', 'Redis', 'Neo4j', 'DynamoDB',

        # 🗃️ Data Pipelines & ETL/ELT
        'Apache Airflow', 'Luigi', 'DBT', 'Apache NiFi', 'Kafka', 'Apache Beam', 'Apache Flink',
        'Fivetran', 'Stitch', 'Matillion', 'Informatica', 'Talend', 'Singer', 'Pentaho', 'StreamSets',

        # 🧪 Experimentation & Statistics
        'A/B Testing', 'Bayesian', 'Frequentist', 'T-Tests', 'P-Values', 'Hypothesis Testing', 'Randomized Control Trials',
        'Statsmodels', 'Prophet', 'PyMC3', 'SciPy', 'RStan',

        # 🧠 NLP & Text Analytics
        'spaCy', 'NLTK', 'Gensim', 'BERT', 'T5', 'LLaMA', 'OpenAI', 'LangChain', 'TextBlob', 'transformers',

        # 📦 DevOps, CI/CD, and Containers
        'Docker', 'Kubernetes', 'Terraform', 'Git', 'GitHub Actions', 'Jenkins', 'CircleCI', 'Ansible', 'ArgoCD',

        # 🛠 MLOps & Workflow Orchestration
        'MLflow', 'Kubeflow', 'Tecton', 'ZenML', 'Dagster', 'SageMaker Pipelines', 'Metaflow', 'Weights & Biases',

        # 📡 APIs & Web Services
        'REST API', 'GraphQL', 'gRPC', 'OpenAPI', 'FastAPI', 'Flask', 'Django',

        # 🔐 Security, Monitoring & Governance
        'Apache Ranger', 'Great Expectations', 'Monte Carlo', 'DataDog', 'Alation', 'Collibra', 'DataHub', 'Bigeye',

        # 📁 File Formats & Serialization
        'Parquet', 'Avro', 'ORC', 'Feather', 'CSV', 'JSON', 'XML', 'YAML', 'HDF5', 'Pickle',

        # 🧠 Data Science Notebooks & IDEs
        'Jupyter', 'VS Code', 'JupyterLab', 'Colab', 'Zeppelin', 'RStudio', 'PyCharm',

        # 🏢 Business Tools
        'Excel', 'Google Sheets', 'Smartsheet', 'Airtable', 'SAP', 'Salesforce',

        # 🛡️ Operating Systems & Scripting
        'Linux', 'Shell', 'Bash', 'PowerShell', 'zsh',

        # 🔁 Scheduling & Workflow Tools
        'Apache Oozie', 'Prefect', 'Crontab', 'RunDeck', 'Control-M'
    ]



    def extract_tools(text):
        if not isinstance(text, str):
            return []
        found = [tool for tool in key_tools if re.search(r'\b' + re.escape(tool) + r'\b', text, flags=re.IGNORECASE)]
        return found

    jobs_df['extracted_tools'] = jobs_df['job_description'].apply(extract_tools)



    def parse_relative_date(text):
        if not isinstance(text, str):
            return pd.NaT
        text = text.lower()
        now = datetime.now()

        patterns = {
            'hour': r'(\d+)\s+hour',
            'day': r'(\d+)\s+day',
            'week': r'(\d+)\s+week',
            'month': r'(\d+)\s+month',
            'year': r'(\d+)\s+year',
            'minute': r'(\d+)\s+minute'
        }

        for unit, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                value = int(match.group(1))
                if unit == 'hour':
                    return (now - timedelta(hours=value)).date()
                elif unit == 'day':
                    return (now - timedelta(days=value)).date()
                elif unit == 'week':
                    return (now - timedelta(weeks=value)).date()
                elif unit == 'month':
                    return (now - timedelta(days=30*value)).date() 
                elif unit == 'year':
                    return (now - timedelta(days=365*value)).date() # Approximate
                elif unit == 'minute':
                    return (now - timedelta(minutes=value)).date()

        return pd.NaT


    jobs_df['time_posted'] = jobs_df['time_posted'].apply(parse_relative_date)
    jobs_df['time_posted'] = pd.to_datetime(jobs_df['time_posted']).dt.date

    db_params = {
        "dbname": "postgres",
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": "5432"
    }



    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # SQL Insert Query
        insert_query = """
        INSERT INTO linkedin_jobs (
            job_id, job_title, company_name, work_model, location,
            time_posted, num_applicants, xp_level, job_type,
            job_sectors, job_description, extracted_tools
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_id) DO NOTHING;  -- Avoids duplicate primary key errors
    """

        
        # Insert DataFrame records one by one
        for _, row in jobs_df.iterrows():
            cursor.execute(insert_query, (
                row['job_id'],
                row['job_title'],
                row['company_name'],
                row['work_model'],
                row['location'],
                row['time_posted'],  # Make sure this is a `date` object
                row['num_applicants'],
                row['xp_level'],
                row['job_type'],
                row['job_sectors'],
                row['job_description'],
                row['extracted_tools']  # Should be a Python list, e.g., ['Python', 'SQL']
        ))

        # Commit and close
        conn.commit()
        print(f"{job_name} Data inserted successfully!")

    except Exception as e:
        print(e)

    finally:
        if conn:
            cursor.close()
            conn.close


for i in jobsss:
    print(f'cool down period before extracting {i} ')
    time.sleep(10)
    get_data(i)