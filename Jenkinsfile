pipeline {
    agent any

    environment {
        REPO_URL = 'https://github.com/ganasai88/ETL-Spark.git' // GitHub repository URL4
        VENV_DIR = 'venv'
        S3_DIR = 'S3-Terraform'
        EMR_DIR = 'EMR'
        ACCESS_KEY = credentials('AWS_ACCESS_KEY_ID')
        SECRET_KEY = credentials('AWS_SECRET_KEY_ID')
        REGION = 'us-east-2'
        S3_BUCKET = 'etlspark'
        STEP_NAME = 'Run Monthly Spark Job'
    }

    stages {
        stage('Checkout') {
            steps {

                git branch: 'main', url: "${REPO_URL}"

            }
        }
        stage('Upload Files to S3') {
            steps {
                 script {
                     sh '''
                     sudo apt install zip
                     echo "Zipping all .py files in monthly/22-11-2024..."

                     # Create a zip file containing all .py files in the directory
                     zip -r monthly/22-11-2024/py_files_22-11-2024.zip monthly/22-11-2024/*.py

                     echo "Zip file created successfully: py_files_22-11-2024.zip"
                     echo "Uploading files to S3 bucket ${S3_BUCKET}..."
                     # Sync the code to S3
                     aws s3 sync . s3://${S3_BUCKET} --exclude ".git/*"
                     echo "Files uploaded successfully!"
                     '''
                 }
            }
        }
        stage('Find Running EMR Cluster') {
            steps {
                script {
                    // Get the cluster ID of the first running EMR cluster
                    def clusterId = sh(
                        script: '''
                        aws emr list-clusters \
                            --active \
                            --query "Clusters[?Status.State=='WAITING']|[0].Id" \
                            --region $REGION \
                            --output text
                        ''',
                        returnStdout: true
                    ).trim()
                    if (!clusterId) {
                        error "No running EMR cluster found!"
                    }
                    echo "Found EMR Cluster ID: ${clusterId}"
                    env.CLUSTER_ID = clusterId
                }
            }
        }
    }
}
