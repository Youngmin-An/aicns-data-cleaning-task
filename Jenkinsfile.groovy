def docker_build = """
apiVersion: v1
kind: Pod
metadata:
  name: jenkins-kaniko-test
spec:
  containers:
  - name: kaniko
    image: gcr.io/kaniko-project/executor:v1.6.0-debug
    command:
    - sleep
    args:
    - 99d
    volumeMounts:
    - name: jenkins-docker-cfg
      mountPath: /kaniko/.docker
  volumes:
    - name: jenkins-docker-cfg
      projected:
        sources:
        - secret:
            name: docker-regcred
            items:
            - key: .dockerconfigjson
              path: config.json

"""

pipeline {
    agent {
        kubernetes {
            yaml docker_build
        }
    }
    environment {
        SLACK_CHANNEL = '#jenkins'
    }
    stages {
        stage('Notify start') {
            steps {
                slackSend (channel: SLACK_CHANNEL, color: '#FFFF00', message: "STARTED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
            }
        }
        stage('Image Build and Push') {
            
            steps {
                git url: 'https://github.com/Youngmin-An/aicns-data-cleaning-task.git', branch: 'develop'
                container('kaniko'){
                    sh "pwd"
                    sh "ls -ls"
                    sh "ls -ls /"
                    sh '/kaniko/executor -f `pwd`/Dockerfile -c `pwd` --insecure --skip-tls-verify --cache=true --destination=youngminan/aicns-data-cleaning-task:latest'
                }
            }
        }
    }
    post {
        success {
            slackSend (channel: SLACK_CHANNEL, color: '#00FF00', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
        failure {
            slackSend (channel: SLACK_CHANNEL, color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
    }
}

