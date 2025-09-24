# buzzline-05-data-git-hub

Nearly every streaming analytics system stores processed data somewhere for further analysis, historical reference, or integration with BI tools.

In this example project, we incorporate relational data stores. 
We stream data into SQLite and DuckDB, but these examples can be altered to work with MySQL, PostgreSQL, MongoDB, and more.

We use one producer that can write up to four different sinks:

- to a file
- to a Kafka topic (set in .env)
- to a SQLite database
- to a DuckDB database

In data pipelines:

- A **source** generates records (our message generator). 
- A **sink** stores or forwards them (file, Kafka, SQLite, DuckDB). 
- An **emitter** is a small function that takes data from a source and writes it into a sink. 
- Each emitter has one job (`emit_message` to the specified sink). 

Explore the code to see which aspects are common to all sinks and which parts are unique.

--- 

## First, Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first.
**Python 3.11 is required.**

## Second, Copy This Example Project & Rename

1. Once the tools are installed, copy/fork this project into your GitHub account
   and create your own version of this project to run and experiment with.
2. Name it `buzzline-05-yourname` where yourname is something unique to you.

Additional information about our standard professional Python project workflow is available at
<https://github.com/denisecase/pro-analytics-01>. 
    
---

## Task 0. If Windows, Start WSL

Launch WSL. Open a PowerShell terminal in VS Code. Run the following command:

```powershell
wsl
```

You should now be in a Linux shell (prompt shows something like `username@DESKTOP:.../repo-name$`).

Do **all** steps related to starting Kafka in this WSL window.

---

## Task 1. Start Kafka (using WSL if Windows)

In P2, you downloaded, installed, configured a local Kafka service.
Before starting, run a short prep script to ensure Kafka has a persistent data directory and meta.properties set up. This step works on WSL, macOS, and Linux - be sure you have the $ prompt and you are in the root project folder.

1. Make sure the script is executable.
2. Run the shell script to set up Kafka.
3. Cd (change directory) to the kafka directory.
4. Start the Kafka server in the foreground. Keep this terminal open - Kafka will run here.

```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

**Keep this terminal open!** Kafka is running and needs to stay active.

For detailed instructions, see [SETUP_KAFKA](https://github.com/denisecase/buzzline-02-case/blob/main/SETUP_KAFKA.md) from Project 2. 

---

## Task 2. Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment.
2. Activate the virtual environment.
3. Upgrade pip and key tools. 
4. Install from requirements.txt.

### Windows

Open a new PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).
**Python 3.11** is required for Apache Kafka. 

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

Open a new terminal in VS Code (Terminal / New Terminal)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

---

## Task 3. Run Tests and Verify Emitters

In the same terminal used for Task 2, we'll run some tests to ensure that all four emitters are working fine on your machine.  All tests should pass if everything is installed and set up correctly. 

```shell
pytest -v
```

Then run the `verify_emitters.py` script as a module to check that we can emit to all four types. 
For the Kakfa sink to work, the Kafka service must be running. 

### Windows Powershell

```shell
py -m verify_emitters
```

### Mac / Linux

```shell
python3 -m verify_emitters
```

---

## Task 4. Start a New Streaming Application

This will take two terminals:

1. One to run the producer which writes messages using various emitters. 
2. Another to run each consumer. 

### Producer Terminal (Outputs to Various Sinks)

Start the producer to generate the messages. 

The existing producer writes messages to a live data file in the data folder.
If the Kafka service is running, it will try to write the messages to a Kafka topic as well.
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.\.venv\Scripts\Activate.ps1
py -m producers.producer_case
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_case
```

NOTE: The producer will still work if the Kafka service is not available.

### Consumer Terminal (Various Options)

Start an associated consumer. 
You have options. 

1. Start the consumer that reads from the live data file.
2. Start the consumer that reads from the Kafka topic.
3. Start the consumer that reads from the SQLite relational data store. 
4. Start the consumer that reads from the DuckDB relational data store.

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.\.venv\Scripts\Activate.ps1
py -m consumers.kafka_consumer_case
OR
py -m consumers.file_consumer_case
OR
py -m consumers.sqlite_consumer_case.py
OR
py -m consumers.duckdb_consumer_case.py
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.kafka_consumer_case
OR
python3 -m consumers.file_consumer_case
OR
python3 -m consumers.sqlite_consumer_case.py
OR
python3 -m consumers.duckdb_consumer_case.py
```

---

## Review the Project Code

Review the requirements.txt file. 
- What - if any - new requirements do we need for this project?
- Note that requirements.txt now lists both kafka-python and six. 
- What are some common dependencies as we incorporate data stores into our streaming pipelines?

Review the .env file with the environment variables.
- Why is it helpful to put some settings in a text file?
- As we add database access and passwords, we start to keep two versions: 
   - .env 
   - .env.example
 - Read the notes in those files - which one is typically NOT added to source control?
 - How do we ignore a file so it doesn't get published in GitHub (hint: .gitignore)

Review the .gitignore file.
- What new entry has been added?

Review the code for the producer and the two consumers.
 - Understand how the information is generated by the producer.
 - Understand how the different consumers read, process, and store information in a data store?

Compare the consumer that reads from a live data file and the consumer that reads from a Kafka topic.
- Which functions are the same for both?
- Which parts are different?

What files are in the utils folder? 
- Why bother breaking functions out into utility modules?
- Would similar streaming projects be likely to take advantage of any of these files?

What files are in the producers folder?
- How do these compare to earlier projects?
- What has been changed?
- What has stayed the same?

What files are in the consumers folder?
- This is where the processing and storage takes place.
- Why did we make a separate file for reading from the live data file vs reading from the Kafka file?
- What functions are in each? 
- Are any of the functions duplicated? 
- Can you refactor the project so we could write a duplicated function just once and reuse it? 
- What functions are in the sqlite script?
- What functions might be needed to initialize a different kind of data store?
- What functions might be needed to insert a message into a different kind of data store?

---

## Explorations

- Did you run the kafka consumer or the live file consumer? Why?
- Can you use the examples to add a database to your own streaming applications? 
- What parts are most interesting to you?
- What parts are most challenging? 

---

## Verify DuckDB (Terminal Commands)

Windows PowerShell

```shell
# count rows
duckdb .\data\buzz.duckdb "SELECT COUNT(*) FROM streamed_messages;"

# peek
duckdb .\data\buzz.duckdb "SELECT * FROM streamed_messages ORDER BY id DESC LIMIT 10;"

# live analytics
duckdb .\data\buzz.duckdb "SELECT category, AVG(sentiment) FROM streamed_messages GROUP BY category ORDER BY AVG(sentiment) DESC;"
```

macOS/Linux/WSL

```shell
# count rows
duckdb data/buzz.duckdb -c "SELECT COUNT(*) FROM streamed_messages;"

# peek
duckdb data/buzz.duckdb -c "SELECT author, COUNT(*) c FROM streamed_messages GROUP BY author ORDER BY c DESC;"

# live analytics
duckdb data/buzz.duckdb -c "SELECT category, AVG(sentiment) FROM streamed_messages GROUP BY category ORDER BY AVG(sentiment) DESC;"

```

---

## How To Stop a Continuous Process

To kill the terminal, hit CTRL c (hold both CTRL key and c key down at the same time).

## Later Work Sessions

When resuming work on this project:

1. Open the project repository folder in VS Code. 
2. Start the Kafka service (use WSL if Windows) and keep the terminal running. 
3. Activate your local project virtual environment (.venv) in your OS-specific terminal.
4. Run `git pull` to get any changes made from the remote repo (on GitHub).

## After Making Useful Changes

1. Git add everything to source control (`git add .`)
2. Git commit with a -m message.
3. Git push to origin main.

```shell
git add .
git commit -m "your message in quotes"
git push -u origin main
```

## Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

## License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.

## Recommended VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter + Formatter)
- **SQLite Viewer by Florian Klampfer**
- WSL by Microsoft (on Windows Machines)


# P5: Database Integration with Streaming Pipelines


## Introduction
In this project, my custom consumer (`consumers/consumer_data_git_hub.py`) reads live JSON events from the Kafka topic `buzzline` and processes each message individually as it arrives. For every record it (1) validates the exact (author, message) pair against a reference corpus built from files/much_ado_excerpt.json, (2) scans the message for “flag words” loaded from `files/flag_words.txt` (Shakespearean, negative/ill-will terms), and (3) checks whether the author appears in `files/bad_authors.txt`. The consumer persists a compact, per-message result into SQLite (`data/buzz.sqlite`, table `author_validation`) including the source timestamp, author, message, author-match boolean, keyword hit count and list, and a `bad_author_flag`. When a message meets the alert policy (configurable via env vars `ALERT_ON_VALID_AUTHOR_ONLY` and `ALERT_KEYWORD_MIN`), it also writes an entry to the `alerts` table and emits a console warning. The reference/flag/bad-author files and the DB path are auto-discovered under `PROJECT_ROOT/files` and `PROJECT_ROOT/data` (via the existing utils config), and the consumer is idempotent and append-only—no need to drop the database between runs.


## Tasks
1. Clone / open the project in VS Code.
2. Create & activate a Python 3.11 virtual environment.
3. Install dependencies from requirements.txt.
4. Create the files folder & place data assets
   C:\Projects\buzzline-05-data-git-hub\files\much_ado_excerpt.json (your provided JSON)
   C:\Projects\buzzline-05-data-git-hub\files\flag_words.txt (negative/ill-will words)
   C:\Projects\buzzline-05-data-git-hub\files\bad_authors.txt (one author per line)
5. (Optional) Create a .env at project root
   If you want to tweak behavior without editing code:
```shell
   KAFKA_BROKER_ADDRESS=127.0.0.1:9092
BUZZ_TOPIC=buzzline
BUZZ_CONSUMER_GROUP_ID=buzz_group
MESSAGE_INTERVAL_SECONDS=5
ALERT_ON_VALID_AUTHOR_ONLY=False
ALERT_KEYWORD_MIN=1
```
(If you don’t use .env, defaults in utils_config apply.)
6. Start Kafka/ZooKeeper (if not already running)
   - Make sure your local Kafka broker is up on 127.0.0.1:9092.
7. Run the producer (Much Ado random author + random message)
```shell
# from project root
py -m producers.producer_much_ado
```
You should see logs like:
```shell
Starting Much Ado Producer (random author + random message).
{'message': '...', 'author': '...', 'timestamp': '...'}
```
8. Run the consumer (in a second terminal)
```shell
py -m consumers.consumer_data_git_hub
```
You should see:
   - DB path confirmation: ... Using SQLite at: PROJECT_ROOT/data/buzz.sqlite
   - Reference/flag/bad-author loads from PROJECT_ROOT/files/...
   - “Processed row” logs for each message
   - If an alert triggers, a [ALERT] ... warning and a row in the alerts table
9. Verify SQLite outputs
   - DB file: C:\Projects\buzzline-05-data-git-hub\data\buzz.sqlite
   - Tables:
      - author_validation (one row per message)
      - alerts (rows only when policy matches)
   - Quick check (use any SQLite browser) and confirm new rows append on each run (the consumer does not drop tables).
10. README.md updates
   - Document how to run:
     - Producer: py -m producers.producer_much_ado
     - Consumer: py -m consumers.consumer_data_git_hub
   - Explain the insight: author/message validation, flag word hits, bad-author flag, and alert policy.
   - Note the file paths you placed under /files and the DB under /data.
11. Git workflow
```shell
git add .
git commit -m "Add custom consumer, producer_much_ado, flag/bad-author files, and README updates"
git push origin <your-branch-or-main>
```

## Requirements

- Python 3.11
- A local virtual environment (.venv)
- Install dependencies
```shell
pip install -r requirements.txt
```

## Troubleshooting
   - No rows in `alerts`: ensure `flag_words.txt` exists and `ALERT_KEYWORD_MIN=1`. If `ALERT_ON_VALID_AUTHOR_ONLY=True`, alerts require BOTH a keyword hit and a valid `(author,message)` pair from `much_ado_excerpt.json`.
   - Old DB schema: if you previously ran before we added columns, the consumer will auto-migrate columns on start. If you see schema errors, stop both apps, close any DB viewers (to release SQLite locks), then rerun the consumer.
   - Windows log file locks: Each process writes its own log: `logs/project_log_<PID>.log.` That avoids rename/rotation conflicts.

## Authors

Contributors names and contact info <br>
@github.com/Data-Git-Hub <br>

---

## Version History
- P5 Main 3.1 | Modify README.md
- P5 Main 3.1 | Modify consumer_data_git_hub.py, README.md 
- P5 Main 3.0 | Add bad_author.txt; Modify consumer_data_git_hub.py - add ability to track bad authors and add to database, README.md
- P5 Main 2.3 | Modify consumer_data_git_hub.py, README.md
- P5 Main 2.2 | Modify multiple files in app.
- P5 Main 2.1 | Modify consumer_data_git_hub.py, README.md
- P5 Main 2.0 | Modify requirements.txt - add vaderSentiment for sentiment value scores, consumer_data_git_hub.py, README.md
- P5 Main 1.7 | Modify much_ado_excerpt.json, producer_much_ado.py, README.md
- P5 Main 1.6 | Modify consumer_data_git_hub.py, README.md
- P5 Main 1.5 | Modify consumer_data_git_hub.py, flag_words.txt, README.md
- P5 Main 1.4 | Delete stopwords.txt; Modify flag_words.txt, README.md
- P5 Main 1.3 | Add flag_words.txt, stopwords.txt; Modify README.md
- P5 Main 1.2 | Add files folder, consumer_data_git_hub.py; Modify README.md
- P5 Main 1.1 | Modify README.md
- P5 Main 1.0 | Modify README.md


## Test History