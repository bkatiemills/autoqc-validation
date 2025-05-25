docker network create iquod
docker container run -d --name iquoddb --network iquod --volume iquodback:/data/db mongo:5.0.27
docker image build -t iquod/validation:autoqc autoqc/
docker image build -t iquod/validation:ncei ncei/
