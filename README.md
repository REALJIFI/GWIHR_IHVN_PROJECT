# GWIHR_IHVN_PROJECT
 
         GWIHR_IHVN ASPIRE PROJECT(RIVERS STATE)

PROJECT OVERVIEW:
Aspire Project
The footprints of our impact in improving the healthcare system are indelibly planted across the country, where we have worked and currently work in implementing several health programs.
The ACTION to Sustain Precision and Integrated HIV Response towards Epidemic Control (ASPIRE) project, which we implement, is funded by the US President’s Emergency Plan for AIDS Relief (PEPFAR) through the US Centres for Disease Control and Prevention (CDC).
The project is currently focused on four states: Nasarawa State, Rivers State, Katsina State, and the Federal Capital Territory. It is aimed at
•   Strengthening laboratory capacity to achieve quality clinical management for HIV and other health challenges.
•   Empowering quality healthcare and a sustainable culture of excellence in HIV management through Continuous Quality Improvement.
•   Increasing the uptake of prevention services, including Pre-Exposure Prophylaxis (PrEP), among those at risk, especially key populations and underserved groups such as adolescents.
•   Strengthening laboratory capacity to achieve quality clinical management for HIV and other health challenges.
•   Anti-retroviral Treatment (ART) for adults and children (including pregnant women)
•   Care and support for people living with HIV/AIDS and people affected by HIV/AIDS
• Monitoring and evaluating patients and program progress.
•   Support to Orphans and Vulnerable Children (OVC),
•   Gender-Based Violence (GBV) support services
•   Training in all the above areas.
ASPIRE  project provides the following services in the health facilities we support: HIV Testing Services (HTS), laboratory diagnosis and tracking of status of people living with HIV, prevention of Mother-to-Child Transmission (PMTCT) of HIV, Anti-retroviral Treatment (ART) for adult and children (including pregnant women), care and support for people living with HIV/AIDS and people affected by HIV/AIDS, monitoring and evaluation of patients and program progress, support to Orphans and Vulnerable Children (OVC), Gender-Based Violence (GBV) support services and training in all the above areas.
CLEAR PROJECT PURPOSE AND SCOPE
This repository supports the ASPIRE Project (Rivers State), led by IHVN, which aims to control the epidemic of HIV. It includes:
•	ETL pipelines for processing patient-level health data from multiple sources (PostgreSQL, CSV).
•	SQL scripts for treatment gap identification, retention analysis, and linkage-to-care evaluation.
•	Data analysis notebooks for KPI tracking, viral load suppression, and ART adherence patterns.
•	Dashboards for visualization of key metrics using tools like Power BI and Grafana.

PROJECT CHALLENGES:
Project Challenges
1.	Missed Appointments
Identify patients who have missed the last two ART appointments.
2.	ART Adherence Monitoring
Measure the timeliness of ART medication pickup over the last 6 months.
3.	Viral Load Suppression
Find patients on ART >6 months with viral load >1000 copies/mL.
4.	New Patient Linkage
Track % of newly diagnosed patients linked to ART within 30 days.
5.	12-Month Retention
Determine ART retention rate after 12 months.
6.	HTS Performance
Compare HIV tests to positive outcomes.
DATA DESCRIPTION
Note: The data used in this project is anonymized to protect patient confidentiality and is used in compliance with ethical standards.
•	patients.csv: Demographics, ART start dates
•	Appointments. csv: ART refill records
•	Lab_Results.csv: Viral load and CD4 counts
•	Hts.csv: HIV testing data
•	Ovc.csv: Orphans and vulnerable children support data

   TOOLS & TECHNOLOGIES
•	Languages: Python, SQL
•	Database Platform: PostgreSQL
•	Big Data: Apache Spark (optional)
•	Orchestration: Airflow, Azure Data Factory
•	Visualization: Power BI, Grafana

    



