/* Build and deploy the Spark library of routines (normalization, state, etc)
 * from the Dewey repository on every merge into master https://github.com/healthverity/dewey
 */

pipeline {

    agent { node { label 'ubuntu-18.04-worker' } }

    environment {
        SLACK_CHANNEL = '#data_platform'

        // Namespaces docker-compose to not compete with concurrent builds
        COMPOSE_PROJECT_NAME = "dewey-spark-deploy-${env.BRANCH_NAME}-${currentBuild.id}"

        // AWS Settings
        AWS_DEFAULT_REGION = 'us-east-1'
        AWS_ACCESS_KEY_ID = sh(script: "curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/jenkins_worker | jq -r '.AccessKeyId'", , returnStdout: true).trim()
        AWS_SECRET_ACCESS_KEY = sh(script: "curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/jenkins_worker | jq -r '.SecretAccessKey'", , returnStdout: true).trim()
        AWS_SESSION_TOKEN = sh(script: "curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/jenkins_worker | jq -r '.Token'", , returnStdout: true).trim()
    }

    stages {

        stage('Build') {
            steps {
                sh 'make package-spark'
            }
        }

        stage('Upload to S3') {
            steps {
                sh 'aws s3 cp dewey_spark.tar.gz healthverityreleases/dewey --sse'
                sh 'aws s3 cp spark/target/dewey.zip healthverityreleases/dewey --sse'
            }
        }
    }

    post {

        success {
            slackSend (color: '#00FF00',
                message: "SUCCESSFUL\nJob: '${env.BUILD_TAG}'\nView build: (${env.BUILD_URL})",
                channel: "${SLACK_CHANNEL}")
        }

        failure {
            slackSend (color: '#FF0000',
                message: "FAILED\nJob: '${env.BUILD_TAG}'\nView build: (${env.BUILD_URL})",
                channel: "${SLACK_CHANNEL}")
        }

        aborted {
            slackSend (color: '#FF9500',
                message: "ABORTED\nJob: '${env.BUILD_TAG}'\nView build: (${env.BUILD_URL})",
                channel: "${SLACK_CHANNEL}")
        }

        cleanup {
            cleanWs();
        }
    }
}
