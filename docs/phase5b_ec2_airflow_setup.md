# Phase 5b — Deploying Airflow on a lightweight EC2 Instance

This guide covers how to spin up a tiny, `$0.02/hour` EC2 instance to run Apache Airflow, avoiding the `$350/month` cost of Amazon MWAA while still keeping the orchestration entirely in the AWS Cloud.

## Step 1: Create the IAM Role for the EC2 Instance

Airflow needs permissions to tell EMR Serverless to run jobs and to pass the EMR Execution Role to it.

1. **Go to AWS Console → IAM → Policies → Create Policy**
2. Click **JSON** and paste the contents of `infrastructure/iam_policies/airflow_ec2_policy.json` (I've just added this to your folder).
3. Name the policy: `Airflow_EC2_Policy`
4. **Go to IAM → Roles → Create Role**
5. Select **AWS service**, choose **EC2**, and click Next.
6. Search for and attach the `Airflow_EC2_Policy` you just created.
7. Name the role: `Airflow_EC2_Role` and create it.

---

## Step 2: Launch the EC2 Instance

1. **Go to AWS Console → EC2 → Launch Instance**
2. **Name:** `Airflow-Orchestrator`
3. **OS:** Amazon Linux 2023 AMI
4. **Instance Type:** `t3.small` (Required. `t3.micro` does not have enough RAM to install Airflow).
5. **Key Pair:** Create a new key pair (e.g., `airflow-key`). Download the `.pem` file. You will need this to SSH in.
6. **Network Settings:**
   - Auto-assign Public IP: **Enable**
   - Firewalls / Security Groups: Create a new security group.
   - **Rule 1:** Allow SSH (Port 22) from **My IP**.
   - **Rule 2:** Allow Custom TCP (Port 8080) from **My IP** (This is for the Airflow Web UI).
7. **Advanced Details:**
   - **IAM Instance Profile:** Select the `Airflow_EC2_Role` you created in Step 1.
8. Click **Launch Instance**.

---

## Step 3: Install & Start Airflow

Once the instance is running, SSH into it using the terminal on your Windows machine:
```bash
ssh -i "path/to/airflow-key.pem" ec2-user@<YOUR_EC2_PUBLIC_IP>
```

Run these exact commands in the EC2 terminal to install Airflow:

```bash
# 1. Install Git and Python tools
sudo yum update -y
sudo yum install git python3-pip -y

# 2. Clone your repository so Airflow has the DAG and config
git clone https://github.com/maniprakashthandapani-hub/aws.git data-pipeline

# 3. Create a Python Virtual Environment
python3 -m venv airflow_env
source airflow_env/bin/activate

# 4. Set Airflow Home directory and install Airflow
export AIRFLOW_HOME=~/airflow
pip install "apache-airflow==2.9.1" apache-airflow-providers-amazon --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.9.txt"

# 5. Initialize the Airflow Database
airflow db migrate

# 6. Create an Admin User (so you can log into the web UI)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# 7. Copy your DAG to the Airflow folder
mkdir -p ~/airflow/dags
cp ~/data-pipeline/airflow/dags/daily_emr_etl.py ~/airflow/dags/

# 8. Start the Webserver (in the background)
airflow webserver -p 8080 -D

# 9. Start the Scheduler (in the background)
airflow scheduler -D
```

---

## Step 4: Access the Airflow UI

1. Open your web browser on your Windows machine.
2. Go to: `http://<YOUR_EC2_PUBLIC_IP>:8080`
3. Log in with Username: `admin` / Password: `admin`.

**Final Configuration inside the Airflow UI:**
Because Airflow is running on an EC2 instance, the DAG needs your AWS ARNs.
1. In the Airflow UI, go to **Admin → Variables**.
2. Add the following variables (you can get the values from `pipeline_config.json`):
   - Key: `S3_BUCKET`, Value: `data-pipeline-dev-tmanipra`
   - Key: `EMR_APP_ID`, Value: `00g5ofepb7fr2k0t`
   - Key: `JOB_ROLE_ARN`, Value: `arn:aws:iam::038849867257:role/EMR_Serverless_ExecutionRole`
   - Key: `KMS_KEY_ARN`, Value: `arn:aws:kms:eu-west-2:038849867257:key/95c4b27f-2243-4e8d-a934-22c201b9e84d`

Now, turn the toggle on the `daily_emr_serverless_etl` DAG to **Unpause** it, and click the **Play** button to trigger it manually!
