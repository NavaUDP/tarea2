const kafka = require('kafka-node');

// Configuración del cliente Kafka
const client = new kafka.KafkaClient({
  kafkaHost: 'localhost:9092'
});

// Configuración del productor
const producer = new kafka.Producer(client);

// Manejo del evento 'ready' para enviar mensajes
producer.on('ready', function() {
  // Mensaje a enviar
  const message = 'Hola, mundo!';
  
  // Enviar el mensaje al topic 'test'
  producer.send([{ topic: 'test', messages: [message] }], function(err, result) {
    console.log('Mensaje enviado:', message);
  });
});

producer.on('error', function(err) {
  console.error('Error en el productor:', err);
  process.exit(1); // Sale del proceso con un código de error
});