
@Library('merritt-build-library')
import org.cdlib.mrt.build.BuildFunctions;

// See https://github.com/CDLUC3/mrt-jenkins/blob/main/src/org/cdlib/mrt/build/BuildFunctions.groovy

pipeline {
    /*
     * Params:
     *   tagname
     *   purge_local_m2
     */
    environment {      
      //Branch/tag names to incorporate into the build.  Create one var for each repo.
      BRANCH_CORE = 'main'
      BRANCH_CLOUD = 'main'
      BRANCH_ZK = 'main'

      //working vars
      M2DIR = "${HOME}/.m2-store"
      DEF_BRANCH = "main"
    }
    agent any

    tools {
        // Install the Maven version 3.8.4 and add it to the path.
        maven 'maven384'
    }

    stages {
        stage('Purge Local') {
            steps {
                script {
                  new BuildFunctions().init_build();
                }
            }
        }
        stage('Build Core') {
            steps {
                dir('mrt-core2') {
                  script {
                    new BuildFunctions().build_library(
                      'https://github.com/CDLUC3/mrt-core2.git', 
                      env.BRANCH_CORE, 
                      '-DskipTests'
                    )
                  }
                }
            }
        }
        stage('Build Cloud') {
            steps {
                dir('mrt-cloud') {
                  script {
                    new BuildFunctions().build_library(
                      'https://github.com/CDLUC3/mrt-cloud.git', 
                      env.BRANCH_CLOUD, 
                      '-DskipTests'
                    )
                  }
                }
            }
        }
        stage('Build CDL ZK Queue') {
            steps {
                dir('cdl-zk-queue') {
                  script {
                    new BuildFunctions().build_library(
                      'https://github.com/CDLUC3/cdl-zk-queue.git', 
                      env.BRANCH_ZK, 
                      '-DskipTests'
                    )
                  }
                }
            }
        }
        stage('Build Store') {
            steps {
                dir('mrt-store'){
                  script {
                    new BuildFunctions().build_war(
                      'https://github.com/CDLUC3/mrt-store.git',
                      ''
                    )
                  }
                }
            }
        }

        stage('Archive Resources') { // for display purposes
            steps {
                script {
                  new BuildFunctions().save_artifacts(
                    'mrt-store/store-war/target/mrt-storewar-1.0-SNAPSHOT.war',
                    'mrt-store'
                  )
                }
            }
        }
    }
}