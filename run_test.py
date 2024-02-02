import pandas as pd
import os
import csv
from models.models import Base, SkillModel, JobModel, JobSeekerModel
import pytest
from run import DataLoader

job_test_data = [
    ['id', 'title', 'required_skills'],
    [1, 'Ruby Developer', 'Ruby, SQL, Problem Solving'],
    [2, 'Frontend Developer', 'JavaScript, HTML/CSS, React, Teamwork'],
    [3, 'Backend Developer', 'Java, SQL, Node.js, Problem Solving'],
]

seeker_test_data = [
    ['id', 'name', 'skills'],
    [1, 'Alice Seeker', 'Ruby, SQL, Problem Solving'],
    [2, 'Bob Applicant', 'JavaScript, HTML/CSS, Teamwork'],
]

'''
Expected Result should be like
    jobseeker_id    jobseeker_name     job_id      job_title           matching_skill_count  matching_skill_percent
        1           Alice Seeker        1        Ruby Developer                 3                   100.0
        1           Alice Seeker        3        Backend Developer              2                    50.0
        2           Bob Applicant       2        Frontend Developer             3                    75.0
'''



@pytest.fixture(scope="session")
def create_dummy_csv():
    job_file_path = 'dummy_jobs.csv'
    seeker_file_path = 'dummy_seekers.csv'

    with open(job_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(job_test_data)

    with open(seeker_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(seeker_test_data)

    yield job_file_path, seeker_file_path 

    os.remove(job_file_path)
    os.remove(seeker_file_path)

@pytest.fixture(scope="function")
def in_memory_data_loader():
    data_loader = DataLoader('sqlite:///:memory:')
    
    Base.metadata.create_all(data_loader.db.engine)

    yield data_loader

    Base.metadata.drop_all(data_loader.db.engine)

@pytest.mark.usefixtures("create_dummy_csv")
def test_read_csv_in_chunk(create_dummy_csv):
    job_file, _ = create_dummy_csv
    
    chunks = DataLoader.read_csv_in_chunk(job_file)
    assert isinstance(chunks, pd.io.parsers.TextFileReader)

@pytest.mark.usefixtures("in_memory_data_loader")
def test_update_skills(in_memory_data_loader):
    unique_skills = ['Python', 'SQL', 'Team Work']
    with in_memory_data_loader.db.get_session() as session:
        session.add(SkillModel(name='Python'))
        session.commit()
        in_memory_data_loader.update_skills(unique_skills)
        assert len(unique_skills) == len(session.query(SkillModel).all())

@pytest.mark.usefixtures("in_memory_data_loader", "create_dummy_csv")
def test_load_file_into_db(in_memory_data_loader, create_dummy_csv):
    job_file, job_seeker_file = create_dummy_csv
    in_memory_data_loader.load_file_into_db(job_file, job_seeker_file)
    with in_memory_data_loader.db.get_session() as session:
        jobs = session.query(JobModel).all()
        seekers = session.query(JobSeekerModel).all()
        assert len(jobs) == len(job_test_data) - 1
        assert len(seekers) == len(seeker_test_data) - 1

        ruby_developer_job = session.query(JobModel).filter(
            JobModel.title=="Ruby Developer").first()

        assert set([s.name for s in ruby_developer_job.skills]) == set(['Ruby', 'SQL', 'Problem Solving'])


@pytest.mark.usefixtures("in_memory_data_loader", "create_dummy_csv")
def test_generate_result_sql(in_memory_data_loader, create_dummy_csv):
    job_file, job_seeker_file = create_dummy_csv
    in_memory_data_loader.load_file_into_db(job_file, job_seeker_file)
    df = in_memory_data_loader.generate_result(True)
    # check result order
    assert df['jobseeker_id'].to_list() == [1, 1, 2]

    alice_ruby_row = df[(df['jobseeker_name'] == 'Alice Seeker') & (df['job_title'] == 'Ruby Developer')]
    alice_backend_row = df[(df['jobseeker_name'] == 'Alice Seeker') & (df['job_title'] == 'Backend Developer')]

    # check percentage
    assert alice_ruby_row['matching_skill_percent'].values[0] == round(3 * 100 / 3, 2)
    assert alice_backend_row['matching_skill_percent'].values[0] == round(2 * 100 / 4, 2)


@pytest.mark.usefixtures("in_memory_data_loader", "create_dummy_csv")
def test_generate_result_no_sql(in_memory_data_loader, create_dummy_csv):
    job_file, job_seeker_file = create_dummy_csv
    in_memory_data_loader.load_file_into_db(job_file, job_seeker_file)
    df = in_memory_data_loader.generate_result(False)
    # check result order
    assert df['jobseeker_id'].to_list() == [1, 1, 2]
    alice_ruby_row = df[(df['jobseeker_name'] == 'Alice Seeker') & (df['job_title'] == 'Ruby Developer')]
    alice_backend_row = df[(df['jobseeker_name'] == 'Alice Seeker') & (df['job_title'] == 'Backend Developer')]

    # check percentage
    assert alice_ruby_row['matching_skill_percent'].values[0] == round(3 * 100 / 3, 2)
    assert alice_backend_row['matching_skill_percent'].values[0] == round(2 * 100 / 4, 2)
