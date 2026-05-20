module MyMod

export greet, add

greet(name::AbstractString) = "hello, $(name), from Julia $(VERSION)"
add(a::Real, b::Real)       = a + b

end
