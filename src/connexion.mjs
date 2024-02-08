import { Kafka } from "kafkajs";

const kafka = new Kafka({
    brokers: ['192.168.214.128:19092'] // Remplacez par l'adresse de votre broker RedPanda
});

const consumer = kafka.consumer({ groupId: 'my-group' });

async function connexion() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'mon-super-topic' }); // Remplacez "votre-topic" par le nom de votre Topic RedPanda
    console.log('Connecté et abonné au Topic RedPanda');
}

connexion();

async function startConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'mon-super-topic' });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const key = message.key ? message.key.toString() : null; // Vérifiez si la clé n'est pas null
            const value = message.value ? message.value.toString() : null; // Vérifiez si la valeur n'est pas null
            console.log({
                topic,
                offset: message.offset,
                value,
                timestamp: new Date().toLocaleString() // Convertit le timestamp en format lisible
            });
        },
    });
}


startConsumer().catch(console.error);
