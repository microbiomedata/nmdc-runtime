(use-modules
 (guix packages)
 (gnu packages autotools)
 (gnu packages check)
 (gnu packages docker)
 (gnu packages jupyter)
 (gnu packages openstack)
 (gnu packages pkg-config)
 (gnu packages python)
 (gnu packages python-build)
 (gnu packages python-check)
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

(packages->manifest
 (list
  automake
  pkg-config
  docker-compose
  jupyter
  python
  python-black
  python-coverage
  python-pytest
  python-jedi
  python-jedi-language-server
  python-ipython
  python-autopep8
  python-flake8
  python-rope
  python-virtualenv
  python-yapf))
