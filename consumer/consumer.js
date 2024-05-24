const kafka = require('kafka-node');

// Configuración del cliente Kafka
const client = new kafka.KafkaClient({
  kafkaHost: 'localhost:9092'
});

// Configuración del consumidor
const consumer = new kafka.Consumer(
  client,
  [{ topic: 'test', partition: 0 }], 
  {
    autoCommit: false
  }
);

// Manejo del evento 'message' para recibir mensajes
consumer.on('message', function(message) {
  console.log('Mensaje recibido:', message.value.toString());
});

consumer.on('error', function(err) {
  console.error('Error en el consumidor:', err);
  process.exit(1); // Sale del proceso con un código de error
});