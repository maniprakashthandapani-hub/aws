# Enterprise Production Architecture: AWS to MongoDB Atlas (Banking Grade)

In a production banking or highly regulated financial environment, **traffic must never traverse the public internet**, and static credentials (usernames/passwords) are strictly forbidden. 

The diagram and sections below describe the industry-standard architecture for securing AWS-to-MongoDB Atlas communication.

---

## 1. Architectural Diagram

```mermaid
flowchart TB
    subgraph AWS VPC ["AWS VPC (Bank-Managed Account)"]
        direction TB
        subgraph Private Subnets ["Isolated Private Subnets (No IGW, No Public IPs)"]
            EMR[EMR Serverless Spark Workers]
            Airflow[Airflow Orchestrator]
            
            EMR -->|TCP 27017| VPCE[AWS PrivateLink VPC Endpoint Interface]
        end
        
        KMS[AWS KMS Key - CMK]
    end

    subgraph Atlas VPC ["MongoDB Atlas VPC (MongoDB-Managed Account)"]
        direction TB
        subgraph Database Shards ["Database Subnets (Completely Private)"]
            Primary[(Primary Shard)]
            Secondary1[(Secondary Shard 1)]
            Secondary2[(Secondary Shard 2)]
        end
        
        VPCEndpointService[Atlas Endpoint Service]
    end

    %% Network Connection
    VPCEndpointService <-->|AWS PrivateLink | VPCE
    Database Shards <--> VPCEndpointService
    
    %% KMS Key BYOK
    KMS -.->|BYOK Envelope Encryption| Database Shards
```

---

## 2. Core Security Pillars of the Production Setup

### 1. Network Transport: AWS PrivateLink (No Internet, No Peering)
* **What it is:** In production, banks avoid standard VPC Peering because it connects routing tables between two VPCs. Instead, they use **AWS PrivateLink**.
* **How it works:** 
  1. You configure a **Private Endpoint** in MongoDB Atlas.
  2. You create an **Interface VPC Endpoint (VPCE)** inside your own AWS private subnets.
  3. All database traffic travels privately over AWS fiber directly from your VPC to Atlas.
  4. Atlas databases appear as local private IP addresses (e.g., `10.0.128.45`) in your subnets.

### 2. Authentication: Passwordless AWS IAM (STS)
* **What it is:** Static passwords are a compliance risk (leakage, rotation overhead).
* **How it works:**
  * The EMR Serverless Execution Role is mapped to a MongoDB Database User in Atlas.
  * When the Spark job initializes, the MongoDB Driver uses AWS Security Token Service (STS) to generate a short-lived cryptographically signed token.
  * Atlas validates this token with AWS IAM. No database username or password ever exists in your code or configurations.

### 3. Encryption at Rest: Bring Your Own Key (BYOK) via KMS
* **What it is:** The bank must retain absolute control of the data keys encrypting the storage volumes in MongoDB Atlas.
* **How it works:**
  * You create a Customer Managed Key (CMK) in AWS KMS.
  * You authorize MongoDB Atlas to access this key via a KMS Key Policy.
  * Atlas uses this key to encrypt the database files at rest. If the bank disables the KMS key, Atlas instantly loses access to read/write the data.

### 4. Network Firewall: IP Whitelisting & Security Groups
* **What it is:** Defense-in-depth security layers.
* **How it works:**
  * Even though traffic goes over PrivateLink, MongoDB Atlas Network Access lists are configured to only allow connections originating from the specific private CIDR blocks of your AWS VPC subnets.
  * The AWS VPC Endpoint Security Group is configured to only accept incoming traffic on port `27017` from your EMR security group (`sg-0b09707e07b0bb688`).
