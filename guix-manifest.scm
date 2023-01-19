(use-modules
 (guix packages)
 (gnu packages base)
 (gnu packages autotools)
 (gnu packages check)
 (gnu packages docker)
 (gnu packages jupyter)
 (gnu packages openstack)
 (gnu packages rust)
 (gnu packages pkg-config)
 (gnu packages python)
 (gnu packages python-build)
 (gnu packages python-check)
 (gnu packages python-compression)
 (gnu packages python-xyz)
 (guix download)
 (guix build-system python)
 ((guix licenses) #:prefix license:))

(define-public python-pygls
  (package
    (name "python-pygls")
    (version "0.12.1")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "pygls" version))
              (sha256
               (base32
                "040nq6a2a0ddv9lq7d6m84a1qixbnv5w5pj7s2p28xkfzlis579v"))))
    (build-system python-build-system)
    (arguments
     `(#:tests? #f
       #:phases
       (modify-phases %standard-phases
         (delete 'sanity-check))))
    (propagated-inputs (list python-cython python-pydantic python-typeguard))
    (native-inputs (list python-bandit
                         python-flake8
                         python-setuptools-scm
                         python-wheel
                         python-mock
                         python-mypy
                         python-pytest
                         python-pytest-asyncio))
    (home-page "https://github.com/openlawlibrary/pygls/tree/master/")
    (synopsis
     "a pythonic generic language server (pronounced like \"pie glass\").")
    (description
     "a pythonic generic language server (pronounced like \"pie glass\").")
    (license license:asl2.0)))

(define-public python-docstring-to-markdown
  (package
    (name "python-docstring-to-markdown")
    (version "0.10")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "docstring-to-markdown" version))
              (sha256
               (base32
                "1xlb2qf2ph6g8xahw7794qz8rhvsxxblpqnrlbzdwwkmgc65pxqj"))))
    (build-system python-build-system)
    (home-page "")
    (synopsis "On the fly conversion of Python docstrings to markdown")
    (description "On the fly conversion of Python docstrings to markdown")
    (license #f)))

(define-public python-jedi-language-server
  (package
    (name "python-jedi-language-server")
    (version "0.37.0")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "jedi-language-server" version))
              (sha256
               (base32
                "006hxy54i1ywmz8hnybb6bfspakq3z2wzvg8y6n70il4apyjkyrg"))))
    (build-system python-build-system)
    (arguments
     `(#:tests? #f
       #:phases
       (modify-phases %standard-phases
         (delete 'sanity-check))))
    (propagated-inputs (list python-docstring-to-markdown
                             python-importlib-metadata python-jedi
                             python-pydantic python-pygls))
    (home-page "https://github.com/pappasam/jedi-language-server")
    (synopsis "A language server for Jedi!")
    (description "This package provides a language server for Jedi!")
    (license license:expat)))

(define-public python-pylsp-mypy
  (package
    (name "python-pylsp-mypy")
    (version "0.6.3")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "pylsp-mypy" version))
              (sha256
               (base32
                "1gf865dj9na7jyp1148k27jafwb6bg0rdg9kyv4x4ag8qdlgv9h6"))))
    (build-system python-build-system)
    (propagated-inputs (list python-lsp-server python-mypy python-toml))
    (native-inputs (list python-coverage python-pytest python-pytest-cov
                         python-tox))
    (home-page "https://github.com/python-lsp/pylsp-mypy")
    (synopsis "Mypy linter for the Python LSP Server")
    (description "Mypy linter for the Python LSP Server")
    (license #f)))

(define-public python-pep517
  (package
    (name "python-pep517")
    (version "0.13.0")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "pep517" version))
              (sha256
               (base32
                "0nczh9pfcin7rlgzgmfw3snypwscp3a2cdr0v6ny2aqpbiy94sdf"))))
    (build-system python-build-system)
    (propagated-inputs (list python-importlib-metadata python-tomli
                             python-zipp))
    (home-page "https://github.com/pypa/pep517")
    (synopsis "Wrappers to build Python packages using PEP 517 hooks")
    (description "Wrappers to build Python packages using PEP 517 hooks")
    (license #f)))

(define-public python-build
  (package
    (name "python-build")
    (version "0.9.0")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "build" version))
              (sha256
               (base32
                "0g5w28ban6k9qywqwdqiqms3crg75rsvfphl4f4qkg8wi57741qs"))))
    (build-system python-build-system)
    (propagated-inputs (list python-colorama python-importlib-metadata
                             python-packaging python-pep517 python-tomli))
    (native-inputs (list python-filelock
                         python-pytest
                         python-pytest-cov
                         python-pytest-mock
                         python-pytest-rerunfailures
                         python-pytest-xdist
                         python-setuptools
                         python-toml
                         python-wheel))
    (home-page "")
    (synopsis "A simple, correct PEP 517 build frontend")
    (description
     "This package provides a simple, correct PEP 517 build frontend")
    (license license:expat)))

(define-public python-pylsp-rope
  (package
    (name "python-pylsp-rope")
    (version "0.1.10")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "pylsp-rope" version))
              (sha256
               (base32
                "1mydh5fp2yz5rayrp3q2ff4y39881wla0sx09cx66bwjbzqh8qcy"))))
    (build-system python-build-system)
    (propagated-inputs (list python-lsp-server python-rope
                             python-typing-extensions))
    (native-inputs (list python-build python-pytest python-twine))
    (home-page "https://github.com/python-rope/pylsp-rope")
    (synopsis
     "Extended refactoring capabilities for Python LSP Server using Rope.")
    (description
     "Extended refactoring capabilities for Python LSP Server using Rope.")
    (license license:expat)))

(define-public python-types-setuptools
  (package
    (name "python-types-setuptools")
    (version "65.5.0.3")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "types-setuptools" version))
              (sha256
               (base32
                "1z59bap6vchjcb5kcsxzgdvdlp1aal3323awn9lxr8pjymqr2xhp"))))
    (build-system python-build-system)
    (home-page "https://github.com/python/typeshed")
    (synopsis "Typing stubs for setuptools")
    (description "Typing stubs for setuptools")
    (license #f)))

(define-public python-types-pkg-resources
  (package
    (name "python-types-pkg-resources")
    (version "0.1.3")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "types-pkg-resources" version))
              (sha256
               (base32
                "1blxmgxrcc2g5g6vqcrpknzzc9m7b4rmv7fr5xb478xy7n6rnjl3"))))
    (build-system python-build-system)
    (home-page "https://github.com/python/typeshed")
    (synopsis "Typing stubs for pkg_resources")
    (description "Typing stubs for pkg_resources")
    (license #f)))

(define-public python-lsp-black
  (package
    (name "python-lsp-black")
    (version "1.2.1")
    (source (origin
              (method url-fetch)
              (uri (pypi-uri "python-lsp-black" version))
              (sha256
               (base32
                "1sfckmajwgil4sqfmkgxmrp7rkz1ybwf5br6rj16msbplfrfmsnp"))))
    (build-system python-build-system)
    (propagated-inputs (list python-black python-lsp-server python-toml))
    (native-inputs (list python-flake8
                         python-isort
                         python-mypy
                         python-pre-commit
                         python-pytest
                         python-types-pkg-resoures
                         python-types-setuptools
                         python-types-toml))
    (home-page "https://github.com/python-lsp/python-lsp-black")
    (synopsis "Black plugin for the Python LSP Server")
    (description "Black plugin for the Python LSP Server")
    (license #f)))

(packages->manifest
 (list
  docker-compose
  gnu-make
  jupyter
  rust
  pkg-config
  python
  python-black
  python-coverage
  python-flake8
  python-jedi
  python-ipython
  python-lsp-server
  python-pydantic
  python-pylsp-mypy
  python-pyflakes
  python-pytest
  python-rope
  python-mccabe
  python-virtualenv
  python-yapf))
