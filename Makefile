.PHONY: $(MAKECMDGOALS)

# Clean Java project
clean:
	gradle clean

# Build the Java library
build: clean
	gradle build

# Publish the project to the local Maven repository
publish-local: build
	gradle publishToMavenLocal

# Publish the project to the Nexus repository
publish: publish-local
	gradle publish