import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';

const client = new grpc.Client();
client.load(['../crates/folo/proto'], 'greet.proto');

export default () => {
    client.connect('10.0.0.8:1234', {
        plaintext: true
    });

    const requestPayload = {
        name: 'K6'
    };

    const response = client.invoke('greet.Greeter/SayHello', requestPayload);

    check(response, {
        'status is OK': (r) => r && r.status === grpc.StatusOK,
    });

    client.close();
};