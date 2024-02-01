from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Table, ForeignKey

Base = declarative_base()

job_skill_association = Table(
    'job_skill',
    Base.metadata,
    Column('job_id', Integer, ForeignKey('jobs.id')),
    Column('skill_id', Integer, ForeignKey('skills.id'))
)

seeker_skill_association = Table(
    'seeker_skill',
    Base.metadata,
    Column('seeker_id', Integer, ForeignKey('seekers.id')),
    Column('skill_id', Integer, ForeignKey('skills.id'))
)

class JobModel(Base):
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    skills = relationship('SkillModel', secondary=job_skill_association)


class JobSeekerModel(Base):
    __tablename__ = 'seekers'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    skills = relationship('SkillModel', secondary=seeker_skill_association)


class SkillModel(Base):
    __tablename__ = 'skills'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
