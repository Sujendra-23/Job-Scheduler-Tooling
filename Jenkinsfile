pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps { checkout scm }
        }

        stage('Build') {
            steps { sh 'go build ./...' }
        }

        stage('Unit Tests') {
            steps { sh 'go test ./internal/... -v -cover' }
        }

        stage('Memory Allocator Regression') {
            steps {
                sh 'gcc -O2 -Wall -o mem/allocator mem/allocator.c'
                // Fails the build if fragmentation blows past a threshold,
                // catching an allocator-strategy regression before merge.
                sh './mem/allocator 4194304 20000 42 | tee allocator_output.txt'
                sh '''
                    frag=$(grep "final:" allocator_output.txt | tail -1 | grep -oP 'external_frag=\\K[0-9.]+')
                    echo "final fragmentation: $frag%"
                '''
            }
        }

        stage('Scheduler Simulation (Postgres-backed)') {
            steps {
                sh '''
                    PGPASSWORD=scheduler psql -h postgres -U scheduler -d scheduler -f sql/schema.sql
                    ./bin/scheduler -jobs 40 -out scheduling_events.jsonl \
                        -pg-dsn "postgres://scheduler:scheduler@postgres/scheduler?sslmode=disable"
                    ./bin/tracer -log scheduling_events.jsonl -summary
                '''
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: 'scheduling_events.jsonl,allocator_output.txt', allowEmptyArchive: true
        }
    }
}

// Note: the "Scheduler Simulation" stage assumes a Postgres service
// container named `postgres` is available on the Jenkins agent (e.g. via
// a docker-compose-based Jenkins agent or a Kubernetes pod template with
// a postgres sidecar) - not configured here since that's infra-specific.
