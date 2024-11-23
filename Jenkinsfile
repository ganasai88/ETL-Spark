/*import groovy.json.JsonSlurper
import java.util.HashMap
*/

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
                                 docker run --rm -e SONAR_HOST_URL=http://3.15.147.90:9000/ \
                                 -e SONAR_LOGIN=sqp_8c2e771f33cbe36bb53dc30cb568db790a2736c7 \
                                 -v /var/lib/jenkins/workspace/Spark-Multi:/usr/src \
                                 sonarsource/sonar-scanner-cli \
                                 -Dsonar.projectKey=ETL-Pyspark \
                                 -Dsonar.sources=. \
                                 -Dsonar.host.url=http://3.15.147.90:9000/ \
                                 -Dsonar.login=sqp_8c2e771f33cbe36bb53dc30cb568db790a2736c7 \
                                 '''
                         }
                     }
        }
        stage('Upload Files to S3') {
            steps {
                 script {
                        sh '''
                            # Change to the directory containing the .py files
                            cd monthly/22-11-2024

                            # Zip the .py files and overwrite if exists
                            zip -jo py_files_22-11-2024.zip *.py
                            echo "Zip file created successfully: py_files_22-11-2024.zip"

                            # Upload everything to S3 bucket, including updated zip and other files, ensuring overwrite
                            echo "Uploading files to S3 bucket ${S3_BUCKET}..."

                            cd ../..

                            # Sync the code to S3, include all files and exclude .git
                            aws s3 sync . s3://${S3_BUCKET}/monthly/22-11-2024/ --exact-timestamps --exclude ".git/*"

                            echo "Files uploaded successfully!"
                        '''
                 }
            }
        }

    }
}
