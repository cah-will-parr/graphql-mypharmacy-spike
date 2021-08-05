const express = require('express');
var elasticsearch = require('elasticsearch');
const { ApolloServer, gql } = require('apollo-server-express');
 
const helloQuery = gql`
  type Query {
    getPatientById(id: Int!): Patient
    getPayors: PayorAgg
  }
`;

const PatientType = gql`
type Patient {
  patient_id: String
  first_name: String
  last_name: String
  date_of_birth: String
  percentage_filled: Int
  phone_number: String
  is_primary_pharmacy: Boolean
  policy: Policy
  work_items: [WorkItem]
  tasks: [String]
  task_count: [String]
}
`

const WorkItem = gql`
type WorkItem {
  id: String
  opportunity_id: String
  status: String
  service: String
  description: String
  expiration_date: String
  display_expiration_date: String
  display_urgency_date: String
  is_primary_pharmacy: Boolean
  opportunity_amount: Int
  policy: Policy
  is_my_pharmacy_opp: Boolean
  pdc: String
}
`


const Policy = gql`
type Policy {
  policy_id: Int
  policy_name:  String
  policy_type:  String
}
`

const PayorAggregation = gql`
type PayorAgg {
  PayorResults: [PayorResult!]!
}
`

const PayorResult = gql`
type PayorResult {
  key: Int!
  doc_count: Int!
}
`



const typeDefs = [helloQuery, PatientType, WorkItem, Policy, PayorAggregation, PayorResult]
 
const resolvers = {
  Query: {
    getPatientById: (_parent, args, ctx) => getPatientById(args),
    getPayors: getPayors,
  },
};



async function getPatient(){
  const response = await esClient.search({
    index: 'patients',
    body: {
      query: {
        match: {
          client_id: 92,
        }
      }
    }
  })
}

async function getPatientById(args){
  const {id} = args
  const response = await esClient.search({
    index: 'patients',
    body: {
      query: {
        match: {
          patient_id: id,
        }
      }
    }
  })
  console.log('get patient by id')
  console.log(JSON.stringify(response.hits.hits[0]._source, null, 2))
  return response.hits.hits[0]._source
}

async function getPayors(){
  const response = await esClient.search({
    index: 'patients',
    body: {
      aggs: {
        payors_agg: {
          terms: {
            field: "policy.policy_id"
          }
        }
      }
    }
  })

  const responseFormatted = response.aggregations['payors_agg'].buckets
  console.log(responseFormatted)
  return {PayorResults: responseFormatted}
}

var esClient = new elasticsearch.Client({
  host: 'localhost:9201',
  log: 'trace',
  apiVersion: '7.x', // use the same version of your Elasticsearch instance
});

esClient.ping({
  // ping usually has a 3000ms timeout
  requestTimeout: 1000
}, function (error) {
  if (error) {
    console.trace('elasticsearch cluster is down!');
  } else {
    console.log('All is well');
  }
});
 
const app = express();
const server = new ApolloServer({ typeDefs, resolvers });
async function startServer(){
  await server.start()
  server.applyMiddleware({ app });
}

startServer()

 
app.listen({ port: 4000 }, () =>
  console.log('Now browse to http://localhost:4000' + server.graphqlPath)
);