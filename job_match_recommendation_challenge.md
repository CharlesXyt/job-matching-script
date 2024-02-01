# Coding Challenge: Job Match Recommendation Engine

Your task is to develop a basic recommendation engine for a job-matching platform. The goal of the engine is to suggest jobs to jobseekers based on their skills and the required skills for each job.

You will be provided with two CSV files:

`jobseekers.csv`: This file contains information about jobseekers. Each row represents a jobseeker and has the following columns:

* `id`: A unique identifier for the jobseeker.
* `name`: The name of the jobseeker.
* `skills`: A comma-separated list of the jobseeker's skills.

`jobs.csv`: This file contains information about jobs. Each row represents a job and has the following columns:

* `id`: A unique identifier for the job.
* `title`: The title of the job.
* `required_skills`: A comma-separated list of skills required for the job.

You should write a program that reads these two CSV files and outputs a list of job recommendations for each jobseeker. Each recommendation should include the jobseeker's ID, the job ID, and the number of matching skills.

The output should be sorted first by jobseeker ID and then by the percentage of matching skills in descending order (so that jobs with the highest percentage of matching skills are listed first). If two jobs have the same matching skill percentage, they should be sorted by job ID in ascending order.

Here's an example of what the output might look like:

```
jobseeker_id, jobseeker_name, job_id, job_title, matching_skill_count, matching_skill_percent
1, Alice, 5, Ruby Developer, 3, 100
1, Alice, 2, .NET Developer, 3, 75
1, Alice, 7, C# Developer, 3, 75
1, Alice, 4, Dev Ops Engineer, 4, 50
2, Bob, 3, C++ Developer, 4, 100
2, Bob, 1, Go Developer, 3, 75
...
```

Your solution should be written in python. Please include instructions for how to run your program and any tests you have written.
Please open a repository (preferred) or email your zipped up code to the hiring manager when you have completed the test.

You will be evaluated on the following criteria:

* Correctness: Does your program correctly match jobseekers to jobs based on their skills?
* Code quality: Is your code easy to understand and maintain?
* Extendibility: If we needed to add additional functionality, how difficult would this be?
* Efficiency: How well does your program handle large inputs?
* Tests: Is your code covered by automated tests?
