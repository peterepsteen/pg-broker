import {PgBroker} from '../index';
import { Exchange } from '../exchange';
import { Pool } from 'pg';
import { Message } from '../message';

//TODO: break out into seperate test suites: https://stackoverflow.com/questions/32751695/how-to-run-jest-tests-sequentially

test('should create tables', async () => {
    await (new PgBroker()).start();
    expect(1).toBe(1)
});

test('should create exchange', async () => {
    const broker = new PgBroker();
    await broker.start();

    const id = await broker.declareExchange('test');
    console.log('should create exchange', {id})
    expect(id).toBeTruthy()

    const dupe = await broker.declareExchange('test');

    expect(dupe).toEqual(id);
});

test('should create queue', async () => {
    const broker = new PgBroker();
    await broker.start();

    const id = await broker.declareQueue('test_queue_10');
    console.log('should create queue', {id})
    expect(id).toBeTruthy()

    const dupe = await broker.declareQueue('test_queue_10');

    expect(dupe).toEqual(id);
});

test('should send to queue', async () => {
    const broker = new PgBroker();
    try {
        await broker.start();
    } catch (error) {
        console.error('oh no')
        console.error(error)
    }
    const queueName = makeid(10);
    const id = await broker.declareQueue(queueName);
    expect(id).toBeTruthy()
    
    const messagePayload = {hello: 'world'};

    let resolve: (value?: unknown) => void;
    const promise = new Promise(r => {
        resolve = r;
    });

    let calls = 0;
    const hey = (msg: Message) => {
        console.log('---------- MESSAGE', {msg})
        expect(msg).toEqual(messagePayload)
        calls++;
        resolve();
    }

    await broker.subscribe(queueName, hey);
    await broker.subscribe(queueName, hey);
    await broker.subscribe(queueName, hey);

    const messageId = await broker.produce(messagePayload, '', queueName);
    expect(messageId).toBeTruthy()

    const noId = await broker.produce(messagePayload, '', 'queue_doesnt_exist');
    expect(noId).toBeFalsy()

    await promise;
    await new Promise(r => setTimeout(r, 2000));
    expect(calls).toEqual(1);
});


function makeid(length: number) {
   var result           = '';
   var characters       = 'abcdefghijklmnopqrstuvwxyz';
   var charactersLength = characters.length;
   for ( var i = 0; i < length; i++ ) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
   }
   return result;
}
