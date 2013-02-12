Gem::Specification.new do |s|
  s.name        = 'super_queue'
  s.version     = '0.1.1'
  s.date        = "#{Time.now.strftime("%Y-%m-%d")}"
  s.summary     = "An SQS-backed queue structure for ruby that works just like a normal queue, except it's essentially infinite and can use very little memory."
  s.description = "A an SQS-backed queue."
  s.authors     = ["Jon Stokes"]
  s.email       = 'jon@jonstokes.com'
  s.files       = ["lib/super_queue.rb"]
  s.homepage    = 'https://github.com/jonstokes/super_queue'

  s.add_dependency('aws-sdk', '~>1.6.5')
end
