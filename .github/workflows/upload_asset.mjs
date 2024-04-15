const owner = process.argv[2]
const repo = process.argv[3]
const releaseId = process.argv[4]
const filename = process.argv[5]
const contentType = process.argv[6]
const baseUrl = process.argv[7]

import {Octokit} from '@octokit/core'

console.log('upload_asset.js', owner, repo, releaseId, filename, contentType)

const octokit = new Octokit({
    auth: process.env.GH_TOKEN,
})

octokit.request(`POST /repos/${owner}/${repo}/releases/${releaseId}/assets?name=${filename}`, {
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
}).then(() => console.log('finished')).catch(console.error)
