# Contributing

## Issues

When opening an issue:

* include the full **backtrace** with your error
* list versions you are using: Rust, Redis, etc.

It's always better to include more info rather than less.

## Code

It's always best to open an issue before investing a lot of time into a
fix or new functionality.  Functionality must meet my design goals and
vision for the project to be accepted; I would be happy to discuss how
your idea can best fit into Cwab.

### Local development setup

#### Recommended

If you install [direnv](https://direnv.net/) and [nix](https://github.com/DeterminateSystems/nix-installer). You just need to `cd` into this directory and run `direnv allow`. After it's done, you're development environment is setup.

#### Non-recommended, but works

Install the [stable rust toolchain](https://rustup.rs/) and openssl and [redis](https://redis.io/)

### Beginner's Guide to Local Development Setup

#### 1. Fork [cwabcorp/cwab](https://github.com/cwabcorp/cwab) project repository to your personal GitHub account

#### 2. Click 'Clone or Download' button in personal cwab repository and copy HTTPS URL

#### 3. On local machine, clone repository

```
git clone HTTPS-URL-FOR-PERSONAL-CWAB-REPOSITORY
```

#### 4. Navigate to your local machine's cwab directory

```
cd cwab/
```

#### 5. Set remote upstream branch

```
git remote add upstream https://github.com/cwabcorp/cwab.git
```

#### 6. Install necessary gems for development and start Redis server

```
cargo check
```

```
redis-server
```

#### 7. Run stuff

```
cargo run librarian start
```

#### 8. Create feature branch and start contributing!

```
git checkout -b new_feature_name
```

### 9. Keep your forked branch up to date with changes in main repo
```
git pull upstream main
```

## Legal

By submitting a Pull Request, you disavow any rights or claims to any changes
submitted to the Cwab project and assign the copyright of
those changes to First Kind Software Inc.

If you cannot or do not want to reassign those rights (your employment
contract for your employer may not allow this), you should not submit a PR.
Open an issue and someone else can do the work.

This is a legal way of saying "If you submit a PR to us, that code becomes ours".
99.9% of the time that's what you intend anyways; we hope it doesn't scare you
away from contributing.
