# EMA-DAP

## Branches

### Hierarchy

The hierarchy of branches is as follows:
- `main` : This branch (as the name suggests) is the main branch based on which other branches fork from. The main branch is expected to have an infinite lifetime. **No development is done on this branch**.
- `dev` : This is a Level 1 branch (child of the `main` branch). Whilst called the development branch, **no development is done on this branch**, other than occasional the creation (and perioidic merging) of Level 2 branches during the development phase.
- `f-ca`: This is a Level 2 branch. It is a parent of feature branches used for the development of Container Apps.
- `f-fa`: This is a Level 2 branch. It is a parent of feature branches used for the development of Function Apps. Refer to the section on [Function Apps](#function-apps) for additional information.
- `f-ui`: This is a Level 2 branch. It is a parent of feature branches used for the development of the DAP web app, also referred to as "*the landing page*" in DAP.
- `f-misc` : This is a Level 2 branch. It is a feature branch, used for all miscellaneous (*mostly pre-emptive*) modifications. Each set of miscellaneous modifications will have a child branch under the `f-misc` branch. Refer to the [section](#miscellaneous) for additional information.

### Function Apps

- `f-fa-fileuploadlistener`:
Function App configured to be triggered whenever a file is uploaded into a configured container within an Azure Storage Account.
- `f-fa-sftp-client`: Function App configured to ingest files from a configured SFTP server.

### Miscellaneous

As mentioned in the section on [branch hierarchy](#hierarchy), each set of miscellaneous (*mostly pre-emptive*) modifications will have a child branch under the `f-misc` branch.

Each miscellaneous modification has an identifier in the format `YYYYMMDD-{sequence-number}`
> For example, all work related to the miscellaneous modification with identifier `20250227-1` will be committed under the branch `f-misc-20250227-1`.
