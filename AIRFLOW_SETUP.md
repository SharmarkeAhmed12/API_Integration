# AIRFLOW SETUP GUIDE

## What We Built (Like I'm a Kid Version)

You now have:

1. **Airflow Webserver** (Port 8080)
   - A dashboard where you can see all your jobs
   - Like a control panel for your weather pipeline

2. **Airflow Scheduler**
   - The manager that watches the clock
   - Says "Hey, it's time to fetch weather!" every 24 hours

3. **Airflow metadata (SQLite for now)**
   - Remembers recent runs in a local SQLite DB for testing
   - We'll upgrade to PostgreSQL later for production

4. **Weather Ingestion DAG**
   - A recipe that says: "Do step 1, 2, 3..."
   - Gets weather → sends to Kafka → saves to database/datalake

---

## How to Run It

### Step 1: Start Everything
```bash
docker-compose up -d
```

This starts:
- Zookeeper
- Kafka
- Airflow Webserver
- Airflow Scheduler

### Step 2: Check the Dashboard
Go to: **http://localhost:8080**

Login with:
- Username: `admin`
- Password: `admin`

### Step 3: Turn On the DAG
1. Look for "weather_ingestion_daily" in the DAG list
2. Click the ON/OFF toggle on the left to turn it ON
3. You'll see it scheduled to run

### Step 4: See It Running
- The DAG will run on the schedule (every 24 hours by default)
- Or click the "play" button to run it immediately
- Watch the logs in real-time

---

## What Happens Behind the Scenes

When the DAG runs:

```
Airflow Scheduler says: "Time to run weather_ingestion_daily!"
                    ↓
Airflow Worker runs the task:
    1. Load cities from cities.txt
    2. Fetch weather from OpenWeather API
    3. Send to Kafka topic "weather_raw"
    4. Save to SQLite database
    5. Save raw JSON to datalake
                    ↓
Task finishes → Logs saved → You can see results in dashboard
```

---

## Common Things to Check

### Logs
Click on a task run → "Log" tab → see what happened

### Monitor Health
```bash
docker-compose logs -f airflow-scheduler  # Watch scheduler
docker-compose logs -f airflow-webserver  # Watch webserver
docker-compose logs -f kafka              # Watch Kafka
```

### Stop Everything
```bash
docker-compose down
```

---

## Next Steps (What We'll Do Later)

✅ DONE: Airflow + Kafka integration
⏭️ TODO: Spark jobs to transform data
⏭️ TODO: PostgreSQL for structured storage
⏭️ TODO: Advanced monitoring

---

## Troubleshooting

**Q: DAG not showing up in Airflow?**
- Check: Are DAG files in `./dags/` folder? ✓
- Refresh the web page (takes 30 seconds to scan)
- Check logs: `docker-compose logs airflow-scheduler`

**Q: Tasks failing?**
- Click task → "Logs" tab
- Check Kafka is running: `docker-compose ps`
- Check API key in Secrets.env

**Q: Can't connect to Kafka?**
- In Docker containers, use `kafka:9092`
- From host machine, use `localhost:9092`
- DAG uses `kafka:9092` (internal container address)

---

Enjoy your automated pipeline! 🎉
