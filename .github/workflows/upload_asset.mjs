const owner = process.argv[2]
const repo = process.argv[3]
const releaseId = process.argv[4]
const filename = process.argv[5]
const contentType = process.argv[6]
const baseUrl = 'https://' + process.argv[7]

import {Octokit} from '@octokit/core'

const auth = process.env.GH_TOKEN

const url = `POST /repos/${owner}/${repo}/releases/${releaseId}/assets?name=${filename}`

const options = {
    baseUrl,
    owner,
    repo,
    release_id: releaseId,
    data: '@' + filename,
    headers: {
        'Accept': 'application/vnd.github+json',
        'Content-Type': contentType,
        'X-GitHub-Api-Version': '2022-11-28',
    },
}

new Octokit({auth}).request(url, options)
    .then(() => console.log('finished'))
    .catch((e) => {
        console.error(e)
        process.exit(1)
    })
