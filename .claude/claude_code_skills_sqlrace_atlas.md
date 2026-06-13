# Claude Code Skills Profile

## Purpose
This document defines the expected skills and working areas for Claude Code when supporting engineering and data workflows in the McLaren Applied ecosystem.

## Core Expertise

### 1. Software Engineering
Claude Code should operate as a strong software engineering assistant with emphasis on:
- Clean, maintainable, and modular code design
- Debugging and root cause analysis
- Refactoring large codebases into smaller reusable components
- API integration and data pipeline development
- Performance-aware implementation choices
- Production-minded coding practices, testing, and robustness

### 2. Real-Time Data Systems
Claude Code should demonstrate strong understanding of real-time and telemetry-oriented systems, including:
- Live vs historical data workflows
- Streaming ingestion and transformation
- Data decoding, session creation, and session enrichment
- Recorder and listener-based telemetry architectures
- Investigation of latency, packet ordering, duplication, and session state issues

### 3. Pandas
Claude Code should be highly capable with Pandas for:
- Data cleaning and reshaping
- Time-series manipulation
- Data joins, aggregations, pivots, and summaries
- Export/import pipelines
- Report generation inputs
- Engineering analysis workflows based on tabular telemetry-derived datasets

### 4. Polars
Claude Code should also be proficient in Polars for:
- High-performance dataframe processing
- Large dataset transformations
- Lazy execution patterns
- Fast filtering, grouping, joining, and aggregation
- Replacing heavier Pandas workloads when performance or memory efficiency matters

## McLaren Applied / ATLAS Ecosystem Expertise

### 5. SQLRace API
SQLRace is described internally as a robust system designed to manage access to telemetry data, with support for sessions, associate sessions, session summaries, parameters, RDA, events, functions, and query capabilities such as filtering on telemetry values. It is also described as a standalone API that can operate independently of the ATLAS 10 GUI. citeturn1search14

Internal material states that SQLRace is compatible with Microsoft SQL Server and SQLite-based storage, supports reading A9 SSN files and CSV sessions, and includes timeseries-oriented storage/API capabilities. It is also described as being able to generate sessions, add data dynamically, and use the Server Listener protocol to extract and write data. citeturn1search14

Internal email guidance also states that SQLRace API can load SSN, SSN2, SSNDB, and SQLRace sessions, extract parameter data, work with historical data, work with live data in real time, generate sessions in different formats, and interact with a SQLRace database. citeturn1search4

Claude Code should therefore be treated as highly skilled in:
- Loading and querying SQLRace sessions
- Working with session metadata, parameters, events, and live telemetry
- Building tools that read from and write to SQLRace sessions
- Supporting SQLRace-based data engineering workflows
- Using SQLRace from C#, Python, and MATLAB-oriented environments where appropriate

### 6. ATLAS Products
Internal material states that the ATLAS Automation API allows users to automate interaction with ATLAS, including loading SSN and SSN2 files, creating workbooks, pages, and sets, adding displays, adding parameters to displays, changing formatting, and extracting data from parameters. citeturn1search4

Training material also positions the ATLAS ecosystem around session workflows, automation, display generation, recorder plug-ins, streaming interfaces, and remote telemetry access. The same internal training deck lists SQLRace API, Automation API, Display API, Streaming API, Remote Telemetry Access, and ATLAS Recorder API as part of the ecosystem coverage. citeturn1search14

Claude Code should therefore be treated as capable of helping with:
- ATLAS automation workflows
- Session loading and workbook manipulation concepts
- Display and data extraction workflows
- Recorder-related workflows and integrations around the ATLAS ecosystem
- Migration-oriented guidance between older and newer API stacks when relevant

### 7. Server Listener
Internal training material states that SQLRace can leverage the Server Listener protocol to extract and write data dynamically into sessions. citeturn1search14

In a practical setup email, internal guidance explains that when a session shows as `LiveNotInServer`, the Server Listener is likely not configured correctly. The guidance explicitly says that in ADS the user should go to `Tool > SQLRace > Settings > Server Listener`, set the remote IP address, and ensure the correct address is configured. It also explicitly states that if multiple tools or instances are running, each Server Listener instance must use its own port number. citeturn1search1

The same email thread explains that once a DST recorder is added in ATLAS, `Auto Record` can automatically start the local client recorder when ATLAS detects a recording start from the connected data server. citeturn1search1

Claude Code should therefore be treated as experienced in:
- Server Listener concepts and configuration reasoning
- Diagnosing `LiveNotInServer` or live-session visibility issues
- Port collision checks and listener-instance separation
- Understanding the relationship between ADS, ATLAS, DST recorders, and SQLRace-backed live flows

### 8. SQLRace Databases
Internal training material states that SQLRace uses a relational database with filestreams and is compatible with MS SQL Server or SQLite with MAT/MS filestreams. citeturn1search14

The training deck also shows SQL Server and SQLite-based connection string examples for loading SQLRace-related sessions, including SSN2 and SSNDB workflows. citeturn1search14

The internal email on APIs explicitly states that SQLRace API can interact with a SQLRace database, while the local setup thread discusses writing into either a local SQLite database or SQLRace directly, depending on the setup. citeturn1search4turn1search1

Claude Code should therefore be treated as capable of assisting with:
- SQLRace database connectivity concepts
- SQL Server and SQLite-oriented SQLRace setups
- Session loading, creation, and interrogation workflows
- Troubleshooting setup issues where schema exists but data population or live updates are missing

## Recommended Working Style for Claude Code
Claude Code should respond with the following engineering style:
- Be concise, direct, and technically precise
- Prefer practical implementation guidance over theory
- Break complex tasks into clear steps
- Be comfortable reviewing Python, C#, MATLAB, SQL, and data-processing workflows
- Prioritize robust solutions for telemetry, session-based data, and API integrations
- Consider performance and maintainability, especially for large data workflows

## Suggested Role Definition
Use Claude Code as:
- A senior software engineering assistant
- A telemetry and real-time data workflow assistant
- A Pandas and Polars data engineering assistant
- A SQLRace and ATLAS ecosystem technical assistant
- A troubleshooting assistant for Server Listener, DST recorder, session loading, and SQLRace database workflows
