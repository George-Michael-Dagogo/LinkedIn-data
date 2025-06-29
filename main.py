import random
import requests

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
import os
title = 'data'
location = 'America'

work_model = {'1': 'on-site', '2': 'Remote', '3': 'Hybrid'}
job_ids = set()
job_work_model = {}

for n_results in range(0,21,10):
    id_WM = random.choice(list(work_model.keys()))
    list_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={title}&location=America&geoId=103644278&f_WT={id_WM}&position=1&start={n_results}'
    print(id_WM)
    print(n_results)
    response = requests.get(list_url)

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
    job_url = f'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}?_l=en_US'
    job_response = requests.get(job_url)
    # print(job_response.status_code)

    if job_response.status_code == 200:
        job_post = {}
        job_soup = BeautifulSoup(job_response.text, 'html.parser')

        job_post['job_id'] = job_id

        try:
            job_post['job_title'] = job_soup.find(
                'h2', {'class': 'top-card-layout__title'}
            ).text.strip()
        except:
            job_post['job_title'] = None

        try:
            job_post['company_name'] = job_soup.find(
                'a', {'class': 'topcard__org-name-link'}
            ).text.strip()
        except:
            job_post['company_name'] = None

        job_post['work_model'] = job_work_model.get(job_id, None)

        try:
            job_post['location'] = job_soup.find(
                'span', {'class': 'topcard__flavor topcard__flavor--bullet'}
            ).text.strip()
        except:
            job_post['location'] = None   

        try:
            job_post['time_posted'] = job_soup.find(
                'span', {'class': 'posted-time-ago__text'}
            ).text.strip()
        except:
            job_post['time_posted'] = None

        try:
            job_post['num_applicants'] = job_soup.find(
                'span', {'class': 'num-applicants__caption'}
            ).text.strip()
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
            job_post['job_description'] = job_soup.find(
                'div', {'class': 'show-more-less-html__markup'}
            ).get_text(separator="\n", strip=True)
        except:
            job_post['job_description'] = None

        job_list.append(job_post)
