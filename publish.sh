#!/bin/bash


function error () {
    echo $1 1>&2
    exit -1
}


VERSION=$(python -c "import samsa; print samsa.__version__")

echo "creating tag $VERSION"
git tag $VERSION

echo "pushing tag"
git push origin $VERSION

if [ -f MANIFEST ]; then
    echo "removing manifest file"
    rm MANIFEST
fi

echo "uploading package."
python setup.py sdist upload -r disqus
