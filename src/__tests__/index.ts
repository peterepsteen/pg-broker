import { PgBroker } from '../index'
import { Message } from '../message'

let broker: PgBroker

beforeAll(async done => {
    //also tests table creation
    broker = new PgBroker()
    await broker.start()
    done()
})

test('should create exchange', async () => {
    const exchange1 = makeid(10)
    const exchange2 = makeid(10)
    const id = await broker.declareExchange(exchange1)
    expect(id).toBeTruthy()
    const dupe = await broker.declareExchange(exchange1)
    expect(dupe).toEqual(id)
    const id2 = await broker.declareExchange(exchange2)
    expect(id2).toBeTruthy()
    expect(id2).not.toEqual(id)
})

test('should create queue', async () => {
    const queue1 = makeid(10)
    const queue2 = makeid(10)
    const id = await broker.declareQueue(queue1)
    expect(id).toBeTruthy()
    const dupe = await broker.declareQueue(queue1)
    expect(dupe).toEqual(id)
    const id2 = await broker.declareQueue(queue2)
    expect(id2).toBeTruthy()
    expect(id2).not.toEqual(id)
})

describe('direct to queue', () => {
    const queueName = makeid(10)
    const messagePayload = { hello: 'world' }

    let resolve: (value?: unknown) => void

    const promise = new Promise(r => {
        resolve = r
    })

    let calls = 0
    const handler = (msg: Message) => {
        expect(msg).toEqual(messagePayload)
        calls++
        resolve()
    }

    test('should create', async done => {
        const id = await broker.declareQueue(queueName)
        expect(id).toBeTruthy()
        done()
    })

    test('should subscribe', async done => {
        await broker.subscribe(queueName, handler)
        await broker.subscribe(queueName, handler)
        await broker.subscribe(queueName, handler)
        done()
    })

    test('should send', async done => {
        const messageId = await broker.produce(messagePayload, '', queueName)
        expect(messageId).toBeTruthy()
        const noId = await broker.produce(
            messagePayload,
            '',
            'queue_doesnt_exist'
        )
        expect(noId).toBeFalsy()
        done()
    })

    test('should only receive one message', async done => {
        await promise
        // give some time to make sure no other handlers were called
        await new Promise(r => setTimeout(r, 2000))
        expect(calls).toEqual(1)

        done()
    })
});

describe('exchange', () => {
    const queueName = makeid(10)
    const exchangeName = makeid(10)

    const messagePayload = { hello: 'world' }

    let resolve: (value?: unknown) => void

    const promise = new Promise(r => {
        resolve = r
    })

    let calls = 0
    const handler = (msg: Message) => {
        expect(msg).toEqual(messagePayload)
        calls++
        resolve()
    }

    test('should create', async done => {
        const id = await broker.declareQueue(queueName)
        expect(id).toBeTruthy()
        done()
    })

    test('should create exchange', async done => {
        const id = await broker.declareExchange(exchangeName)
        expect(id).toBeTruthy()
        done()
    })

    test('should bind queue to exchange', async done => {
        await broker.bindQueueToExchange(queueName, exchangeName);
        done()
    })

    test('should subscribe', async done => {
        await broker.declareQueue(queueName)
        await broker.subscribe(queueName, handler)
        await broker.subscribe(queueName, handler)
        await broker.subscribe(queueName, handler)
        done();
    })

    test('should send', async done => {
        const messageId = await broker.produce(messagePayload, exchangeName);
        expect(messageId).toBeTruthy()

        let error;
        try {
            await broker.produce(messagePayload, 'exchange_doesnt_exist')
        } catch (err) {
            error = err
        }
        // standardize this with queue logic
        expect(error).toBeTruthy()
        done()
    })

    test('should only receive one message', async done => {
        await promise
        // give some time to make sure no other handlers were called
        await new Promise(r => setTimeout(r, 2000))
        expect(calls).toEqual(1)

        done()
    })

    test('can support multiple queues', async done => {
        const newExchangeName = makeid(10)
        const firstQueueName = makeid(10)
        const secondQueueName = makeid(10)
        const thirdQueueName = makeid(10)

        let id = await broker.declareQueue(secondQueueName);
        expect(id).toBeTruthy()
        id = await broker.declareQueue(firstQueueName);
        expect(id).toBeTruthy()
        id = await broker.declareQueue(thirdQueueName);
        expect(id).toBeTruthy()
        id = await broker.declareExchange(newExchangeName);
        expect(id).toBeTruthy()

        console.log({firstQueueName, newExchangeName})
        await broker.bindQueueToExchange(firstQueueName, newExchangeName);
        await broker.bindQueueToExchange(secondQueueName, newExchangeName);
        
        let calls = 0
        let resolve1: (value?: unknown) => void
        const promise1 = new Promise(r => {
            resolve1 = r
        })
        const handler1 = (msg: Message) => {
            expect(msg).toEqual(messagePayload)
            calls++
            resolve1();
        }


        let resolve2: (value?: unknown) => void
        const promise2 = new Promise(r => {
            resolve2 = r
        })
        const handler2 = (msg: Message) => {
            expect(msg).toEqual(messagePayload)
            calls++
            resolve2();
        }
        const promises = [promise1, promise2];

        await broker.subscribe(secondQueueName, handler1);
        await broker.subscribe(firstQueueName, handler2);

        //this one shouldnt trigger
        await broker.subscribe(thirdQueueName, handler2);

        const messageId = await broker.produce(messagePayload, newExchangeName);
        expect(messageId).toBeTruthy()
        await Promise.all(promises);

        // give some time to make sure no other handlers were called
        await new Promise(r => setTimeout(r, 2000))
        expect(calls).toEqual(2);
        done()
    })
})

function makeid(length: number) {
    let result = ''
    const characters = 'abcdefghijklmnopqrstuvwxyz'
    const charactersLength = characters.length
    for (let i = 0; i < length; i++) {
        result += characters.charAt(
            Math.floor(Math.random() * charactersLength)
        )
    }
    return result
}
