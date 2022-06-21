#!/usr/bin/env groovy
@Library("com.optum.jenkins.pipeline.library@master") _

String getRepoName() {
    return "$GIT_URL".tokenize('/')[3].split("\\.")[0]
}

String getRepoOwnerName() {
    return "$GIT_URL".tokenize('/')[2].split("\\.")[0]
}

import com.optum.jenkins.pipeline.library.scm.Git
import com.optum.jenkins.pipeline.library.sonar.Sonar

pipeline {
    agent {
        label 'docker-gradle-slave'
    }

    environment {
        APP_NAME = "${getRepoName()}"
        IMAGE = "${getRepoName()}"
        TAG = "${env.BRANCH_NAME}"
        BUILD = "${env.BUILD_NUMBER}"
        IMAGE_TAG = "${IMAGE}/${BRANCH_NAME}"
        IMAGE_TAG_BUILD = "${IMAGE_TAG}-${BUILD}"
        JFROG_CREDS = 'bds_oso_id'
        NAMESPACE = 'bds_oso_id'
        GIT_URL = "https://github.optum.com/${getRepoOwnerName()}/${getRepoName()}.git"
        BRANCH_NAME = "${env.BRANCH_NAME}"
        K8S_CLUSTER = '10.202.2.252'
        K8S_NAME_SPACE = 'cdbbds'
        K8S_CREDENTIALS_ID = 'k8s_token-cdbbds'
        KUBECTL_VERSION = '1.14.2'
        JENKINSFILE_LOCATION = 'Jenkinsfile'

    }

    stages {
      stage('Code Checkout'){
		steps{
		echo '-------- Code checkout --------'
		checkout([$class: 'GitSCM', branches: [[name: '*/master']], doGenerateSubmoduleConfigurations: false,submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'bds_oso_id', url: GIT_URL]]])
 		}
	}
        
        stage('Build') {
            steps {
                sh 'chmod +x gradlew'
                sh './gradlew clean build'
                sh './gradlew assemble'
                stash name: 'test', includes: "build/libs/*"
            }
        }
        stage("Fortify Scan") {
            agent { label 'docker-fortify-slave' }
            steps {
                script {
                    try {
                        last_run_stage = "Sonar and Fortify Scans"
                        glFortifyScan fortifyBuildName: APP_NAME,
                                criticalThreshold: 600,
                                highThreshold: 600,
                                mediumThreshold: 600,
                                lowThreshold: 600,
                                isGenerateDevWorkbook: true,
                                sourceDirectory: env.WORKSPACE,
                                failBuildWhenThresholdPassed: false,
                                archiveArtifacts: true,
                                downloadScan: true,
                                uploadScan: true
                    } catch (err) {
                        echo "Fortify failed: " + err
                    }
                }
            }
        }
        stage('Sonar Scan') {
            steps {
                script {
                    try {
                        glSonarGradleScan productName: getRepoOwnerName(),
                                scmRepoUrl: GIT_URL,
                                sonarServer: "sonar.optum",
                                sonarProjectDescription: APP_NAME,
                                sources: "src",
                                additionalProps: ['sonar.github.disableInlineComments': 'true', 'sonar.java.binaries': '/home/jenkins/workspace/*']
                    } catch (err) {
                        echo "Sonar scan failed: " + err
                    }
                }
            }
        }
        stage('Create Image and Push to Jfrog') {
            steps {
                echo '-------- In the Jfrog Routine --------'
                script {
                    try {
                        glDockerImageBuild containerRegistry: "docker.repo1.uhc.com",
                                credentialsId: JFROG_CREDS,
                                image: "docker.repo1.uhc.com/bds_oso_id/edp/compact/cdbedpstreamprodgroupscluster"
                        glDockerImagePush containerRegistry: "docker.repo1.uhc.com",
                                credentialsId: JFROG_CREDS,
                                image: "docker.repo1.uhc.com/bds_oso_id/edp/compact/cdbedpstreamprodgroupscluster"
                    }
                    catch (err) {
                        echo "Create Image and Push to Jfrog failed: " + err
                    }
                }
            }
        }
        /*
        stage('Deploy') {
            agent { label 'docker-kitchensink-slave' }
            steps {
                git branch: BRANCH_NAME, url: GIT_URL

                glKubernetesApply credentials: K8S_CREDENTIALS_ID,
                        cluster: K8S_CLUSTER,
                        namespace: K8S_NAME_SPACE,
                        env: "NonProd",
                        isProduction: false,
                        yamls: ["yaml/Deployment.yaml"],
                        deleteIfExists: true,
                        wait: true
            }
        }
    }*/
}
}
