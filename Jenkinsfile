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
        stage('SonarQube Analysis') {
                     steps {
                         script {
                                 sh '''
                                 docker run --rm -e SONAR_HOST_URL=http://18.117.71.55:9000/ \
                                 -e SONAR_LOGIN=sqp_8c2e771f33cbe36bb53dc30cb568db790a2736c7 \
                                 -v /var/lib/jenkins/workspace/Spark-Multi:/usr/src \
                                 sonarsource/sonar-scanner-cli \
                                 -Dsonar.projectKey=ETL-Pyspark \
                                 -Dsonar.sources=. \
                                 -Dsonar.host.url=http://18.117.71.55:9000/ \
                                 -Dsonar.login=sqp_8c2e771f33cbe36bb53dc30cb568db790a2736c7 \
                                 '''
                         }
                     }
        }
        stage('Upload Files to S3') {
            steps {
                 script {

                     sh '''
                     cd monthly/22-11-2024 && zip -j py_files_22-11-2024.zip *.py
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
        stage('Add Step to EMR Cluster') {
                    steps {
                        script {
                           // Adding step to the running EMR cluster
                           def addStepCommand = """
                               aws emr add-steps \
                                   --cluster-id ${env.CLUSTER_ID} \
                                   --steps '[{
                                       "Type": "Spark",
                                       "Name": "${STEP_NAME}",
                                       "ActionOnFailure": "CONTINUE",
                                       "Args": [
                                           "--deploy-mode", "cluster",
                                           "--py-files","s3://${S3_BUCKET}/monthly/22-11-2024/py_files_22-11-2024.zip",
                                           "s3://${S3_BUCKET}/monthly/22-11-2024/main.py",
                                           "--json_file_path", "s3a://${S3_BUCKET}/monthly/22-11-2024/configurations/config.json"
                                       ]
                                   }]' \
                                   --region ${REGION}
                           """

                           sh addStepCommand

                           echo "Step added to EMR Cluster ID: ${env.CLUSTER_ID}"


                        }
                    }
                }
    }
}
