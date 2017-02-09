stage('Build'){
    node {
        checkout scm
        packpack = new org.tarantool.packpack()
        packpack.packpackBuildMatrix('result')
    }
}
