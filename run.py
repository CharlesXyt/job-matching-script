import pandas as pd
from config import DATABASE_URL, PANDAS_CHUNK_SIZE, ALEMBIC_CONFIG_FILE
from models.models import JobModel, JobSeekerModel, SkillModel
from db import SQLiteDB
from alembic.config import Config
from alembic import command
from typing import List
import sys


def read_csv_in_chunk(path:str):
    return pd.read_csv(path, chunksize=PANDAS_CHUNK_SIZE)


def update_skills(unique_skills: List):
    db = SQLiteDB(DATABASE_URL)
    with db.get_session() as session:
        existed_skills = session.query(SkillModel).filter(SkillModel.name.in_(unique_skills)).all()

    existed_skills_set = set(s.name for s in existed_skills)
    new_unique_skills_models = [SkillModel(name=skill_name) 
                                for skill_name in unique_skills if skill_name not in existed_skills_set]
    
    with db.get_session() as session:
        session.bulk_save_objects(new_unique_skills_models)
        session.commit()



def load_file_into_db(job_file_path:str, seeker_file_path:str):
    db = SQLiteDB(DATABASE_URL)
    jobs_df = read_csv_in_chunk(job_file_path)
    seekers_df = read_csv_in_chunk(seeker_file_path)
   
    for chunk in jobs_df:
        chunk[['id', 'title']].to_sql(JobModel.__tablename__, db.engine, if_exists='replace', index=False)
        unique_skills = set(chunk['required_skills'].str.split(',').explode().str.strip())
        update_skills(unique_skills)
        with db.get_session() as session:
            for _, row in chunk.iterrows():
                job_id = row['id']
                skills = set(s.strip() for s in row['required_skills'].split(','))
                job_obj = session.query(JobModel).filter(JobModel.id==int(job_id)).first()
                for skill in skills:
                    skill_obj = session.query(SkillModel).filter(SkillModel.name==skill).first()
                    job_obj.skills.append(skill_obj)

            session.commit()

    for chunk in seekers_df:
        chunk[['id', 'name']].to_sql(JobSeekerModel.__tablename__, db.engine, if_exists='replace', index=False)
        unique_skills = set(chunk['skills'].str.split(',').explode().str.strip())
        update_skills(unique_skills)
        with db.get_session() as session:
            for _, row in chunk.iterrows():
                seeker_id = row['id']
                skills = set(s.strip() for s in row['skills'].split(','))
                seeker_obj = session.query(JobSeekerModel).filter(JobSeekerModel.id==int(seeker_id)).first()
                for skill in skills:
                    skill_obj = session.query(SkillModel).filter(SkillModel.name==skill).first()
                    seeker_obj.skills.append(skill_obj)
            session.commit()

def generate_result(use_sql=True):
    db = SQLiteDB(DATABASE_URL)
    if use_sql:
        sql = '''
            select
                seekers."id" as jobseeker_id, 
                seekers."name" as jobseeker_name,
                jobs."id" as job_id, 
                jobs."title" as job_title, 
                count(CASE WHEN job_skill."skill_id" = seeker_skill."skill_id" THEN 1 END) as matching_skill_count, 
                round(count(CASE WHEN job_skill."skill_id" = seeker_skill."skill_id" THEN 1 END) * 100 / count(DISTINCT job_skill."skill_id"), 2)  as matching_skill_percent
            from jobs join job_skill on jobs."id" = job_skill."job_id"
            cross join seekers join seeker_skill on seekers."id" = seeker_skill."seeker_id"
            group by seekers."id", seekers."name", jobs."title"
            having count(CASE WHEN job_skill."skill_id" = seeker_skill."skill_id" THEN 1 END) > 0
            order by 1 asc, 6 desc, 3 asc
        '''
        df = pd.read_sql_query(sql, db.engine)
    else:
        result = []
        with db.get_session() as session:
            seekers = session.query(JobSeekerModel).all()
            jobs = session.query(JobModel).all()
            for seeker in seekers:
                for job in jobs:
                    seeker_skills = set(s.id for s in seeker.skills)
                    job_skills = set(s.id for s in job.skills)
                    match_skills = seeker_skills.intersection(job_skills)
                    if len(match_skills) > 0:
                        result.append({
                            "jobseeker_id": seeker.id, 
                            "jobseeker_name": seeker.name, 
                            "job_id": job.id, 
                            "job_title": job.title, 
                            "matching_skill_count": len(match_skills),
                            "matching_skill_percent":round(len(match_skills) * 100 / len(job_skills), 2)
                        })
        sorted_result = sorted(result, key=lambda x: [x["jobseeker_id"], -x["matching_skill_percent"], x["job_id"]])
        df = pd.DataFrame(sorted_result)

    print(df.to_string(index=False))






def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <command>")
        sys.exit(1)

    input_command = sys.argv[1]

    if input_command not in {'init', 'generate'}:
        print(f"Unknown command: {input_command}")
        sys.exit(1)

    if input_command == 'init':
        alembic_cfg = Config(ALEMBIC_CONFIG_FILE)
        # run db migrations
        command.upgrade(alembic_cfg, "head")
    else:
        if len(sys.argv) < 3:
            print("Error: Please provide two file paths as command-line arguments.")
            print("Usage: python run.py generate <path_to_jobs.csv> <path_to_jobseekers.csv>")
            sys.exit(1)

        load_file_into_db(sys.argv[2], sys.argv[3])
        generate_result()



if __name__ == '__main__':
    main()


