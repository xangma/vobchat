## DDME Prototype: A Conversational AI Dashboard

This project is a prototype for a Dynamic Data and Multimodal Engagement (DDME) system. It combines a chat interface with a map and visualizations to allow users to explore statistical data in a conversational manner.

### Architecture

The system architecture is illustrated in the diagram below:

![High-level architecture diagram](diagram-1.png "High-level architecture diagram")

**Key components:**

* **Plotly Dash Web App:** Provides the user interface (UI) with a chat component, a map component, and a visualization component.
* **LangChain & LangGraph:** Facilitate interaction with the language model (LLM) and manage the workflow logic.
* **Ollama:** An interface to a local language model (LLM) such as Llama.
* **PostgreSQL Database:** Stores the statistical data (in this case, Vision of Britain data).
* **GeoPandas:** Used for handling geospatial data and polygon information.
* **Workflow Logic & Tools:** Custom Python code that defines the workflow steps, database queries, and data processing.

### Workflow

The user interacts with the system through the chat interface. The workflow processes the user's input and responds with information and visualizations. The main steps include:

1. **Query Extraction:** The LLM extracts key information from the user's query, such as place names, themes, and year ranges.
2. **Data Retrieval:** Based on the extracted information, the system queries the database to retrieve relevant data.
3. **Visualization:** The retrieved data is displayed on the map and/or as charts and graphs.
4. **User Interaction:** The user can further refine their query or explore different aspects of the data through the chat interface.

### Chat Streaming & SSE (How it works)

VobChat uses a lightweight Server‑Sent Events (SSE) bridge to stream model output to the browser and keep the UI responsive.

- Endpoints:
  - `GET /sse/<thread_id>`: Long‑lived SSE stream. Emits `state_update`, `interrupt`, and `error` events.
  - `POST /workflow/<thread_id>`: Advances the LangGraph workflow; results stream back to the open SSE connection.
- Event flow:
  - The browser opens the SSE connection for a given `thread_id` and optionally posts an initial `workflow_input` (e.g., a user message).
  - The server sets `llm_busy=True` immediately so the “thinking” indicator shows.
  - When the LLM starts streaming tokens, the adapter forwards incremental `messages` arrays containing the full chat history plus a growing AI bubble. On the first visible token, it clears `llm_busy`.
  - During streaming, snapshot events still flow for other UI keys (map, options, cubes), but snapshot “messages” are suppressed to avoid duplicate updates.
  - When no streaming occurs (e.g., planner‑only turns), snapshots deliver the final `messages` array.
- Stream tagging (server side):
  - Planner and subagent chains are tagged `planner`, `subagent`, and `no_ui_stream` so their JSON/metadata is never shown in chat.
  - Natural chat replies are tagged `reply_stream` so token chunks are forwarded to the UI.
- Conversational agent behavior:
  - If the planner returns `actions=[]` and no `final_reply` (e.g., a simple greeting), the agent still produces a streamed natural reply via a plain chat LLM.
  - If the planner returns a `Chat` action, the agent streams a reply as well. Other actions are routed to their nodes.
- Frontend client (`assets/sse_client.js`):
  - Maintains one EventSource per `thread_id` and updates chat, map, and visualization directly from `state_update` events.
  - Drives the “thinking” indicator via the `llm_busy` flag and ensures message order without duplicates.

Troubleshooting streaming
- No tokens rendering: check that reply chains are tagged `reply_stream`, and planner/subagent chains carry `no_ui_stream`.
- Duplicate message updates: verify the adapter suppresses snapshot `messages` while streaming is active.
- Spinner oddities: ensure `llm_busy` is cleared only after the first visible token renders.

### Code Structure

The code is organized into the following directories and files:

* **app:** Contains the main application code.
    * `main.py`: Initializes the Dash app, defines the layout, and registers callbacks.
    * `workflow.py`: Defines the workflow logic and nodes using LangGraph.
    * `workflow_sse_adapter.py`: Bridges the compiled workflow to SSE, handling token streams and UI deltas.
    * `callbacks`: Contains callback functions for handling user interactions.
        * `chat.py`: Callbacks for the chat interface.
        * `chat_sse.py`: Simplified chat callbacks that coordinate SSE connections and workflow input.
        * `map_leaflet.py`: Callbacks for the map component.
        * `visualization.py`: Callbacks for the visualization component.
        * `clientside_callbacks.py`: Client-side callbacks for UI updates.
    * `components`: Contains UI components.
        * `chat.py`: Defines the chat layout.
        * `map.py`: Defines the map layout.
        * `visualization.py`: Defines the visualization layout.
    * `config.py`: Loads configuration settings.
    * `stores.py`: Manages data stores for the application state.
    * `tools.py`: Provides helper functions for database queries.
    * `utils`: Contains utility functions and constants.
        * `constants.py`: Defines constants for unit types and themes.
        * `polygon_cache.py`: Caches polygon data for faster retrieval.
    * `assets/sse_client.js`: The browser SSE client that renders chat tokens and handles interrupts.
    * `conversational_agent.py`: LLM‑planned agent that emits actions and/or a natural reply.
    * `intent_handling.py`, `intent_subagents.py`: Robust intent extraction using subagents (place/theme/action).

### Installation and Running

#### Local Development
1. Clone the repository.
2. Install the required packages: `pip install -r requirements.txt`.
3. Configure the database connection in `config.py`.
4. Run the app: `python -m vobchat.app`.

#### Docker Installation

For easier deployment, VobChat can be run in a Docker container with Redis included, while connecting to external Ollama and PostgreSQL services.

**Prerequisites:**
- Docker and Docker Compose installed
- Ollama server running on localhost:11434
- PostgreSQL database running on localhost:5432

**Setup:**
1. Clone the repository
2. Copy the environment template: `cp .env.example .env`
3. Update `.env` with your database credentials:
   ```bash
   DB_HOST=host.docker.internal
   DB_PORT=5432
   DB_NAME=vobchat
   DB_USER=postgres
   DB_PASSWORD=your_postgres_password
   SECRET_KEY=your-production-secret-key-here
   ```
4. Build and run: `docker-compose up --build`
5. Create a login user: `docker-compose exec vobchat flask --app vobchat.app:server add-user admin@example.com`
6. Access the application at `http://localhost:8050`

**Running in Background:**
To run the container in the background (detached mode):
```bash
docker-compose up -d --build
```

**Managing the Background Container:**
* View logs: `docker-compose logs -f vobchat`
* Stop container: `docker-compose down`
* Restart container: `docker-compose restart`
* Check status: `docker-compose ps`
* Create user (while running): `docker-compose exec vobchat python create_user.py testuser@email.com password`

**Docker Architecture:**
- **Internal**: Redis server runs inside the container
- **External**: Connects to Ollama (port 11434) and PostgreSQL (port 5432) on host system
- **Networking**: Uses host networking mode for seamless access to host services
- **Logging**: Persistent log directory mounted as volume

**Environment Variables:**
* `DB_HOST`: Database host (default: host.docker.internal)
* `DB_PORT`: Database port (default: 5432)
* `DB_NAME`: Database name (default: vobchat)
* `DB_USER`: Database username (default: postgres)
* `DB_PASSWORD`: Database password
* `OLLAMA_HOST`: Ollama server host (default: host.docker.internal)
* `OLLAMA_PORT`: Ollama server port (default: 11434)
* `REDIS_HOST`: Redis host (default: localhost - internal to container)
* `REDIS_PORT`: Redis port (default: 6379)
* `SECRET_KEY`: Flask secret key for session security (required for login)
* `DATABASE_URL`: User authentication database URL (default: sqlite:///users.db)

**Authentication:**
The application requires user authentication via Flask-Login. Users must log in with email/password before accessing the chat interface. Use the `flask add-user` command to create accounts.

**Ollama Integration:**

`ollama show deepseek-r1:latest --modelfile > deepseekr1_wt.modelfile`

change the model template in that file. Then create a new model file with the command:

```bash
ollama create deepseek-r1-wt --modelfile deepseekr1_wt.model
```

### Future Work

* So much to do!
