#!/usr/bin/env bash
# Prepara Google Colab con Java 11, Apache Kafka 3.7 (modo KRaft, sin ZooKeeper) y
# Hadoop 3.3.6 en modo pseudo-distribuido (HDFS: 1 NameNode + 1 DataNode).
# Es idempotente: si algo ya está instalado o corriendo, no lo repite.
set -e
KAFKA_VER=3.7.2
SCALA_VER=2.13
HADOOP_VER=3.3.6
OPT=/opt
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$OPT/hadoop
export KAFKA_HOME=$OPT/kafka

# ---------- Java 11 ----------
if [ ! -d "$JAVA_HOME" ]; then
  echo ">> Instalando Java 11..."
  apt-get -qq update > /dev/null
  apt-get -qq install -y openjdk-11-jdk-headless > /dev/null
fi

descargar () {   # descargar <archivo> <url1> <url2> ...
  local destino=$1; shift
  [ -s "$destino" ] && return 0
  for url in "$@"; do
    echo "   descargando $url"
    if wget -q --timeout=60 "$url" -O "$destino"; then return 0; fi
    rm -f "$destino"
  done
  echo "No se pudo descargar $destino"; return 1
}

cd $OPT
# ---------- Kafka ----------
if [ ! -d "$KAFKA_HOME" ]; then
  echo ">> Descargando Kafka $KAFKA_VER..."
  descargar kafka.tgz \
    "https://archive.apache.org/dist/kafka/$KAFKA_VER/kafka_$SCALA_VER-$KAFKA_VER.tgz" \
    "https://dlcdn.apache.org/kafka/$KAFKA_VER/kafka_$SCALA_VER-$KAFKA_VER.tgz"
  tar xzf kafka.tgz && mv kafka_$SCALA_VER-$KAFKA_VER kafka && rm kafka.tgz
fi

# ---------- Hadoop ----------
if [ ! -d "$HADOOP_HOME" ]; then
  echo ">> Descargando Hadoop $HADOOP_VER (unos 700 MB, puede tardar unos minutos)..."
  descargar hadoop.tgz \
    "https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz" \
    "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz"
  tar xzf hadoop.tgz && mv hadoop-$HADOOP_VER hadoop && rm hadoop.tgz
fi

# ---------- Configuración HDFS pseudo-distribuido ----------
cat > $HADOOP_HOME/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>/opt/hdfs_data/tmp</value></property>
</configuration>
EOF
cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///opt/hdfs_data/nn</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///opt/hdfs_data/dn</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
</configuration>
EOF
grep -q "PP_CONFIG" $HADOOP_HOME/etc/hadoop/hadoop-env.sh || cat >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh <<EOF
# PP_CONFIG
export JAVA_HOME=$JAVA_HOME
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
EOF

puerto_abierto () { (echo > /dev/tcp/localhost/$1) >/dev/null 2>&1; }

if [ ! -d /opt/hdfs_data/nn/current ]; then
  echo ">> Formateando el NameNode..."
  $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive > /dev/null 2>&1
fi
if ! puerto_abierto 9000; then
  echo ">> Iniciando HDFS (NameNode + DataNode)..."
  $HADOOP_HOME/bin/hdfs --daemon start namenode
  $HADOOP_HOME/bin/hdfs --daemon start datanode
fi
for i in $(seq 1 30); do puerto_abierto 9000 && break; sleep 2; done
$HADOOP_HOME/bin/hdfs dfsadmin -safemode wait > /dev/null 2>&1 || true

# ---------- Kafka en modo KRaft ----------
KCFG=$KAFKA_HOME/config/kraft/server.properties
sed -i 's#^log.dirs=.*#log.dirs=/opt/kafka_data#' $KCFG
if [ ! -f /opt/kafka_data/meta.properties ]; then
  echo ">> Formateando el almacenamiento de Kafka (KRaft)..."
  UUID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
  $KAFKA_HOME/bin/kafka-storage.sh format -t "$UUID" -c $KCFG > /dev/null
fi
if ! puerto_abierto 9092; then
  echo ">> Iniciando el broker de Kafka..."
  KAFKA_HEAP_OPTS="-Xmx1G -Xms512M" $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KCFG
fi
for i in $(seq 1 30); do puerto_abierto 9092 && break; sleep 2; done

echo ""
echo ">> Procesos Java activos (jps):"
$JAVA_HOME/bin/jps
puerto_abierto 9000 && echo "HDFS  OK (hdfs://localhost:9000)" || echo "HDFS  NO RESPONDE"
puerto_abierto 9092 && echo "Kafka OK (localhost:9092)" || echo "Kafka NO RESPONDE"

