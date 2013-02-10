Gem::Specification.new do |s|
  s.name        = 'super_queue'
  s.version     = '0.0.8'
  s.date        = Time.now
  s.summary     = "An SQS-backed queue structure for ruby that works just like a normal queue, except it's essentially infinite and can use very little memory."
  s.description = "A an SQS-backed queue."
  s.authors     = ["Jon Stokes"]
  s.email       = 'jon@jonstokes.com'
  s.files       = ["lib/super_queue.rb"]
  s.homepage    = 'https://github.com/jonstokes/super_queue'

  s.add_dependency('fog', '~>1.9.0')
end
