# 🚀 Neptune Query Shell

Professional multi-language graph database interface for Amazon Neptune with AI-powered natural language querying.

## ✨ Features

- 🤖 **AI-Powered Chat** - Query your database using natural language
- 🔍 **Multi-Language Support** - SPARQL, Gremlin, and OpenCypher (SPARQL ready, others in Phase 2)
- 📊 **Smart Result Display** - Professional tables with automatic truncation for large datasets
- 💾 **Intelligent CSV Export** - AI can export results on demand with natural language requests
- ⚙️ **Schema-Driven** - Customizable for any Neptune database via JSON configuration
- 🔄 **Real-Time Execution** - AI actually executes queries against Neptune (no fabricated results)
- 🛡️ **Context Window Safe** - Smart truncation prevents AI token overflow

## 🏁 Quick Start

### 1. Clone and Setup Virtual Environment

```bash
# Clone the repository
git clone https://github.com/karthiks3000/neptune-query-shell.git
cd neptune-query-shell

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Neptune Database Configuration (REQUIRED)
NEPTUNE_ENDPOINT=your-neptune-cluster.region.neptune.amazonaws.com
NEPTUNE_REGION=us-east-1
NEPTUNE_PORT=8182

# AI Assistant Configuration (REQUIRED for Chat with AI)
BEDROCK_MODEL_ID=us.anthropic.claude-sonnet-4-20250514-v1:0
```

### 3. Setup AWS Credentials

The AI assistant requires AWS credentials for Amazon Bedrock. Choose one method:

**Option A: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_SESSION_TOKEN=your-session-token  # If using temporary credentials
```

**Option B: AWS CLI Configuration**
```bash
aws configure
# Enter your credentials when prompted
```

**Option C: IAM Roles (if running on AWS)**
- Attach appropriate IAM role to your EC2/ECS/Lambda instance

### 4. Enable Bedrock Model Access

1. Go to AWS Console → Amazon Bedrock → Model Access
2. Enable access to **Claude 4 Sonnet** model
3. Wait for approval (usually immediate for Claude models)

### 5. Customize Your Database Schema

Edit `schema/user_schema.json` to match your Neptune database structure:

```json
{
  "database_info": {
    "name": "Your Database Name",
    "description": "Description of your graph database"
  },
  "vertices": [
    {
      "label": "Person",
      "description": "Individual person entity",
      "properties": {
        "id": {"type": "string", "description": "Unique identifier"},
        "name": {"type": "string", "description": "Full name"},
        "age": {"type": "number", "description": "Age in years"}
      }
    }
  ],
  "edges": [
    {
      "label": "KNOWS",
      "description": "Person knows another person",
      "from_vertex": "Person",
      "to_vertex": "Person"
    }
  ]
}
```

### 6. Run the Shell

```bash
python neptune_query_shell.py
```

## 📖 Usage Guide

### Initial Setup Flow

1. **Connection Validation** - Shell automatically validates Neptune connectivity
2. **Language Selection** - Choose SPARQL (default), Gremlin, or OpenCypher  
3. **Interface Selection** - Choose your approach:
   - 📝 **Execute Your Query** - Write and run your own queries
   - 🤖 **Chat with AI** - Describe what you want in natural language

### Natural Language AI Chat

The AI assistant can understand requests like:
- *"Find all people over 30 years old"*
- *"Show me the most connected users in the network"*
- *"Count all relationships of type FRIENDS"*
- *"Export the results to CSV"*

**Example Conversation:**
```
💬 Your request: Find all people in London
🤖 AI: Found 25 people in London (showing first 10):
    [Rich table with results]
    
    I notice most work in tech industry. Would you like to explore by occupation?

💬 Export to CSV
🤖 AI: ✅ Exported all 25 records to london_people_20241025_223045.csv (1.2 MB)

💬 Now show me people in Paris over 35
🤖 AI: [Executes new query with real results...]
```

### Manual Query Execution

Paste your own SPARQL/Gremlin/OpenCypher queries:
```
SELECT ?person ?name ?age
WHERE {
  ?person a :Person .
  ?person :name ?name .
  ?person :age ?age .
  FILTER (?age > 30)
}
```

### Special Commands

- `/reset` - Reset entire Neptune database (requires confirmation)
- `/export` - Export last query results to CSV
- `quit` or `exit` - Exit the shell

## ⚙️ Configuration

### Database Schema Configuration

The `schema/user_schema.json` file defines your database structure for the AI:

```json
{
  "database_info": {
    "name": "Social Network",
    "description": "User connections and interactions"
  },
  "vertices": [
    {
      "label": "User",
      "properties": {
        "userId": {"type": "string", "description": "Unique ID"},
        "name": {"type": "string"},
        "location": {"type": "string", "examples": ["London", "Paris", "NYC"]}
      }
    },
    {
      "label": "Post", 
      "properties": {
        "postId": {"type": "string"},
        "content": {"type": "string"},
        "timestamp": {"type": "string"}
      }
    }
  ],
  "edges": [
    {
      "label": "FOLLOWS",
      "from_vertex": "User",
      "to_vertex": "User"
    },
    {
      "label": "CREATED",
      "from_vertex": "User", 
      "to_vertex": "Post"
    }
  ],
  "rdf_namespaces": {
    "users": "http://mycompany.com/users/",
    "posts": "http://mycompany.com/posts/"
  }
}
```

### Environment Variables Reference

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `NEPTUNE_ENDPOINT` | Neptune cluster endpoint | ✅ Yes | - |
| `NEPTUNE_REGION` | AWS region | ✅ Yes | - |
| `NEPTUNE_PORT` | Neptune port | No | 8182 |
| `BEDROCK_MODEL_ID` | Bedrock model for AI | Yes* | Claude 4 Sonnet |

*Required only if using "Chat with AI" functionality

## 🔧 Development

### Project Structure
```
neptune-query-shell/
├── neptune_query_shell.py          # Main application
├── requirements.txt                # Python dependencies
├── .env                           # Environment configuration (you create)
├── .gitignore                     # Git ignore rules
├── neptune/                       # Neptune client modules
│   ├── __init__.py
│   ├── client.py                  # Neptune client wrapper
│   └── connection.py              # Connection management
├── utils/                         # Utility modules
│   ├── ai_query_generator.py      # AI assistant with Strands Agent SDK
│   ├── csv_exporter.py           # Generic CSV export
│   ├── display_formatter.py      # Rich table formatting
│   └── spinner.py                # Loading animations
├── schema/                        # Database schema configuration
│   ├── user_schema.json          # Your database structure (customize this!)
│   └── examples/                 # Example schemas for different domains
└── templates/                     # AI prompt templates
    ├── system_prompts/
    │   └── base_system.j2         # Core AI instructions
    └── query_languages/
        ├── sparql_instructions.j2     # SPARQL-specific guidance
        ├── gremlin_instructions.j2    # Gremlin patterns
        └── opencypher_instructions.j2 # OpenCypher patterns
```

### Adding New Query Languages

1. Add language to `QueryLanguage` enum in `utils/ai_query_generator.py`
2. Create instruction template in `templates/query_languages/`
3. Add query examples to `schema/user_schema.json`
4. Implement execution method in Neptune client (Phase 2)

## 🛠️ Troubleshooting

### Connection Issues
```
❌ Missing Neptune configuration
```
**Solution**: Create `.env` file with `NEPTUNE_ENDPOINT` and `NEPTUNE_REGION`

```
❌ Connection failed: Access denied
```  
**Solution**: Check AWS credentials have Neptune permissions

### AI Chat Issues
```
❌ AI processing failed
```
**Solution**: Verify AWS credentials and Bedrock model access enabled

```  
❌ Context window overflow
```
**Solution**: This is fixed with smart truncation - report if still occurs

### Import Errors
```
❌ ModuleNotFoundError: No module named 'strands'
```
**Solution**: `pip install -r requirements.txt` in activated virtual environment


## 📝 Example Use Cases

### Exploring a New Database
```
💬 What kind of data is in this database?
🤖 AI: [Explores schema and provides overview]

💬 Show me some sample records
🤖 AI: [Returns sample data with insights]

💬 Export all users to CSV
🤖 AI: ✅ Exported 1,247 users to users_20241025_223045.csv
```

### Data Analysis Workflow
```
💬 Find the most active users
🤖 AI: [Queries for users with most connections/posts]

💬 What's the age distribution?
🤖 AI: [Analyzes age patterns in the data]

💬 Show me users from Europe only
🤖 AI: [Filters by location criteria]
```

### Database Administration
```  
💬 How many records are in the database?
🤖 AI: [Counts all vertices and edges]

/reset
[Confirms and resets entire database]

💬 Import the new dataset and verify
🤖 AI: [Helps validate imported data]
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Make changes and test with your Neptune database
4. Commit changes: `git commit -m 'Add amazing feature'`
5. Push branch: `git push origin feature/amazing-feature`
6. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

- **Issues**: Report bugs via GitHub Issues
- **Documentation**: All templates and schema are in the repository
- **Community**: Contribute improvements via Pull Requests

---

**Made with ❤️ for the Neptune community**
