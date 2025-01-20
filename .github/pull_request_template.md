<!--┌─────────────────────────────────────────────────────────────────────────┐
    │ 1. Summary (required)                                                   │
    │                                                                         │
    │ Summarize the changes you made on this branch. This is typically a more │
    │ detailed restatement of the PR title.                                   │
    │                                                                         │
    │ Example: "On this branch, I updated the `/studies/{study_id}` endpoint  │
    │           so it returns an HTTP 404 response when the specified study   │
    │           does not exist."                                              │
    └─────────────────────────────────────────────────────────────────────────┘-->

On this branch, I...

### Details

<!--┌─────────────────────────────────────────────────────────────────────────┐
    │ 2. Details (optional)                                                   │
    │                                                                         │
    │ Provide additional information you think readers will find useful.      │
    │ Readers include PR reviewers, release note authors, app debuggers, and  │
    │ your future self. Additional information might include motivation,      │
    │ rationale, and a description of how things used to be.                  │
    │                                                                         │
    │ Example: "It previously returned an HTTP 404 response and an empty      │
    │           JSON object."                                                 │
    └─────────────────────────────────────────────────────────────────────────┘-->

...

### Related issue(s)

<!--┌─────────────────────────────────────────────────────────────────────────┐
    │ 3. Related issue(s) (optional)                                          │
    │                                                                         │
    │ Link to any GitHub issue(s) this branch was designed to resolve.        │
    │                                                                         │
    │ Example: "Fixes #12345"                                                 │
    └─────────────────────────────────────────────────────────────────────────┘-->

...

### Related subsystem(s)

<!--┌─────────────────────────────────────────────────────────────────────────┐
    │ 4. Related subsystem(s) (required)                                      │
    │                                                                         │
    │ Mark the checkbox next to each subsystem related to the changes in this │
    │ branch. This information might influence who you request reviews from.  │
    │                                                                         │
    │ Example: If you modified the `/studies/{study_id}` API endpoint,        │
    │          mark the checkbox next to "Runtime API (except the Minter)".   │
    └─────────────────────────────────────────────────────────────────────────┘-->

- [ ] Runtime API (except the Minter)
- [ ] Minter
- [ ] Dagster
- [ ] Project documentation (in the `docs` directory)
- [ ] Translators (metadata ingest pipelines)
- [ ] MongoDB migrations
- [ ] Other

### Testing

<!--┌─────────────────────────────────────────────────────────────────────────┐
    │ 5. Testing (required)                                                   │
    │                                                                         │
    │ Indicate whether you have already tested the changes this branch        │
    │ contains; and, if so, how someone other than you can test them. That    │
    │ may involve attaching example files or ad hoc test instructions.        │
    │                                                                         │
    │ Example: "I tested these changes by adding a pytest test that ensures   │
    │           the database does not contain a Study whose ID is `foo`,      │
    │           then submits an HTTP request to `/studies/foo` and confirms   │
    │           the response status is 404."                                  │
    └─────────────────────────────────────────────────────────────────────────┘-->

- [ ] I tested these changes (explain below)
- [ ] I did not test these changes

I tested these changes by...

### Documentation

<!--┌─────────────────────────────────────────────────────────────────────────┐
    │ 6. Documentation (required)                                             │
    │                                                                         │
    │ Indicate whether, in this branch, you have updated all documentation    │
    │ that would otherwise become inaccurate if this branch were to be        │
    │ merged in.                                                              │
    └─────────────────────────────────────────────────────────────────────────┘-->

- [ ] I **have not checked** for relevant documentation yet (e.g. in the `docs` directory)
- [ ] I have **updated** all relevant documentation so it will remain accurate
- [ ] Other (explain below)

### Maintainability

<!--┌─────────────────────────────────────────────────────────────────────────┐
    │ 7. Maintainability (required)                                           │
    │                                                                         │
    │ Indicate whether you have done each of these things that can make code  │
    │ easier to maintain, whether by your teammates or by your future self.   │
    └─────────────────────────────────────────────────────────────────────────┘-->

- [ ] Every Python function I defined includes a docstring _(test functions are exempt from this)_
- [ ] Every Python function parameter I introduced includes a type hint (e.g. `study_id: str`)
- [ ] All "to do" or "fix me" Python comments I added begin with either `# TODO` or `# FIXME`
- [ ] I used `black` to format all the Python files I created/modified
- [ ] The PR title is in the imperative mood (e.g. "Do X") and not the declarative mood (e.g. "Does X" or "Did X")
